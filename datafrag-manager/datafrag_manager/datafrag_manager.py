import logging
from datetime import datetime

from pyspark.sql.session import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType
from pyspark.sql import functions as F

from .utils import convert_filter_to_dnf, get_metadata_table_representation


logger = logging.getLogger(__name__)

class datafragSparkAPI(object):
    def __init__(self, spark_session: SparkSession, keyspace: str, table: str, table_containment: str, datafrag_warehouse: str) -> None:
        self.spark_session = spark_session
        self.keyspace = keyspace
        self.table = table
        self.table_containment = table_containment
        self.datafrag_warehouse = datafrag_warehouse
    
    def _extract_metadata(self, datafrag_ref: str, dataframe: DataFrame) -> dict:
        operations = dataframe._sc._jvm.PythonSQLUtils.explainString(dataframe._jdf.queryExecution(), 'cost').split('\n\n')[0].replace('== Optimized Logical Plan ==\n','')
        operations = operations.split('\n')
        operations = operations[::-1]
        metadata = metadata = {'dataflow': datafrag_ref, 'operation': {}}
        for ope in operations:
            temp = ope.strip().split(', Statistics')
            if '[' in temp[0][0:]:
                columns = list(
                    set(
                        [column.split('#')[0].strip().replace('[','') if ') AS' not in column else column.split(') AS ')[1].split('#')[0].strip().replace('[','') for column in temp[0][temp[0].find("["):temp[0].rfind("]")].split(",")]
                    )
                )
                columns.sort()
                metadata['operation'].update({
                    f'{temp[0][0:temp[0].find("[")].strip().lower().replace("+- ","")}': {
                        'value': temp[0][temp[0].find("["):],
                        'cost': temp[1].split("=")[1].replace(")",""),
                        'columns': columns
                    }
                })
            else:
                metadata['operation'].update({
                    f'{temp[0][0:temp[0].find("(")].strip().lower().replace("+- ","")}': {
                        'value': temp[0][temp[0].find("("):],
                        'cost': temp[1].split("=")[1].replace(")","")
                    }
                })
        metadata['timestamp'] = datetime.now().replace(microsecond=0)
        logger.debug(metadata)
        return metadata

    def _schema_operation(self) -> StructType:
        schema = StructType([
            StructField('dataflow', StringType()),
            StructField('operation', StringType()),
            StructField('timestamp', TimestampType())
        ])
        return schema
    
    def _schema_table_meta_repr(self) -> StructType:
        schema = StructType([
            StructField('attribute', StringType()),
            StructField('dataflow', StringType()),
            StructField('datasource', StringType()),
            StructField('task', StringType()),
            StructField('term', LongType()),
            StructField('min', DoubleType()),
            StructField('max', DoubleType())
        ])
        return schema

    def put_datafrag(self, datafrag_ref: str, datafrag: DataFrame, task_name: str, datasource: str) -> None:
        metadata = self._extract_metadata(datafrag_ref, datafrag)
        schema = self._schema_operation()
        df_operation = self.spark_session.createDataFrame(data=[metadata], schema=schema)
        if metadata.get('operation', {}).get('filter', {}).get('value'):
            df_containment = get_metadata_table_representation(
                spark_session=self.spark_session,
                dataflow=datafrag_ref,
                task=task_name, 
                datasource=datasource, 
                filter=metadata.get('operation').get('filter').get('value')
            )
            (df_containment.write
                .format('org.apache.spark.sql.cassandra')
                .mode('append')
                .options(table=self.table_containment, keyspace=self.keyspace)
                .save()
            )

        (df_operation.write
            .format('org.apache.spark.sql.cassandra')
            .mode('append')
            .options(table=self.table, keyspace=self.keyspace)
            .save()
        )
        
        (datafrag.write
            .format('delta')
            .mode('overwrite')
            .save(f'{self.datafrag_warehouse}/{datafrag_ref}')
        )

    def get_datafrag(self, datafrag_ref: str) -> DataFrame:
        try:
            datafrag = (self.spark_session.read
                .format('delta')
                .load(f'{self.datafrag_warehouse}/{datafrag_ref}')
            )
            return datafrag
        except Exception as e:
            logger.warning(f'Could not read fragment because: {e}')
            return None
    
    def resolve_containment(self, df_consumer: DataFrame) -> dict:
        df_producer = (self.spark_session.read
            .format('org.apache.spark.sql.cassandra')
            .options(table=self.table_containment, keyspace=self.keyspace)
            .load()
        )
        df_producer.show(5)
        df_producer.createOrReplaceTempView('producer')
        df_consumer.createOrReplaceTempView('consumer')
        result = self.spark_session.sql('''
            WITH prod AS (
                SELECT dataflow, term, COUNT(1) as pattrs FROM producer group by dataflow, term
            )
            
            SELECT prod.dataflow, A.term, count(distinct(attribute)) attrs, pattrs, IF(count(distinct(attribute)) = pattrs,true,false) contain FROM (
                SELECT P.attribute, C.term
                FROM producer P, consumer C
                WHERE
                P.datasource = C.datasource AND
                P.attribute = C.attribute AND
                (((P.min <= C.min) OR (isnull(P.min) = true AND isnull(C.min) = true)) AND
                ((P.max >= C.max) OR (isnull(P.max) = true AND isnull(C.max) = true)))
            ) A, prod
            WHERE A.term = prod.term
            GROUP BY prod.dataflow, A.term, prod.pattrs
        ''').agg(F.max('contain').alias('contain'),F.max('dataflow').alias('dataflow')).head().asDict()
        logger.debug(result)
        return result

class datafragOperationsAPI(object):
    def __init__(self, cassandra_connection, keyspace: str, table: str) -> None:
        self.cassandra_connection = cassandra_connection
        self.keyspace = keyspace
        self.table = table
    
    def have_datafrag_operation(self, datafrag_ref: str) -> bool:
        query = f'SELECT * FROM {self.keyspace}.{self.table} WHERE dataflow=%s'
        future = self.cassandra_connection.execute_async(query, [datafrag_ref])
        try:
            rows = future.result()
            if rows:
                return True
            else:
                return False
        except Exception as e:
            logger.warning(f'Could not read fragment because: {e}')
            return False