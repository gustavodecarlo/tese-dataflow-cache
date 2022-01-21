import logging
from datetime import datetime

from pyspark.sql.session import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

logger = logging.getLogger(__name__)

class datafragSparkAPI(object):
    def __init__(self, spark_session: SparkSession, keyspace: str, table: str, datafrag_warehouse: str) -> None:
        self.spark_session = spark_session
        self.keyspace = keyspace
        self.table = table
        self.datafrag_wareghouse = datafrag_warehouse
    
    def _extract_metadata(self, datafrag_ref: str, dataframe: DataFrame) -> dict:
        operations = dataframe._sc._jvm.PythonSQLUtils.explainString(dataframe._jdf.queryExecution(), "cost").split('\n\n')[0].replace("== Optimized Logical Plan ==\n","")
        operations = operations.split("+-")
        operations = operations[::-1]
        metadata = metadata = {"dataflow": datafrag_ref, "operation": {}}
        for ope in operations:
            temp = ope.strip().split(', Statistics')
            metadata['operation'].update({
                f'{temp[0][0:temp[0].find("[")].strip().lower()}': {
                    'value': temp[0][temp[0].find("["):],
                    'cost': temp[1].split("=")[1].replace(")","")
                }
            })
        metadata['timestamp'] = datetime.now()
        logger.debug(metadata)
        return metadata

    def _schema_operation(self) -> StructType:
        schema = StructType([
            StructField("dataflow", StringType()),
            StructField("operation", StringType()),
            StructField("timestamp", TimestampType())
        ])
        return schema

    def put_datafrag(self, datafrag_ref: str, datafrag: DataFrame) -> None:
        metadata = self._extract_metadata(datafrag_ref, datafrag)
        schema = self._schema_operation()
        df_operation = self.spark_session.createDataFrame(data=[metadata], schema=schema)
        (df_operation.write
            .format('org.apache.spark.sql.cassandra')
            .mode('append')
            .options(table=self.table, keyspace=self.keyspace)
            .save()
        )
        (datafrag.write
            .format('delta')
            .mode('overwrite')
            .save(f'{self.datafrag_wareghouse}/{datafrag_ref}')
        )

    def get_datafrag(self, datafrag_ref: str) -> DataFrame:
        datafrag = (self.spark_session.read
            .format('delta')
            .load(f'{self.datafrag_wareghouse}/{datafrag_ref}')
        )
        return datafrag
    