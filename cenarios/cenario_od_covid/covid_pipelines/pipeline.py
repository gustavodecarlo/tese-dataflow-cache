import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from datafrag_manager import datafragSparkAPI


logger = logging.getLogger(__name__)


def pipeline_covid_raw_ingest(
    spark_session: SparkSession,
    bucket: str,
    warehouse: str,
    source_data: str,
    destiny_table: str
) -> None:
    logger.info(f'Read source data {source_data}')
    source_df = (spark_session.read
        .format('csv')
        .option('header', 'true')
        .load(source_data)
    )

    source_df = (source_df.withColumn("FIPS",col("FIPS").cast("integer"))
        .withColumn("Lat",col("Lat").cast("double"))
        .withColumn("Long_",col("Long_").cast("double"))
        .withColumn("Confirmed",col("Confirmed").cast("integer"))
        .withColumn("Deaths",col("Deaths").cast("integer"))
        .withColumn("Recovered",col("Recovered").cast("integer"))
        .withColumn("Active",col("Active").cast("integer"))
        .withColumn("Incident_Rate",col("Incident_Rate").cast("double"))
        .withColumn("Case_Fatality_Ratio",col("Case_Fatality_Ratio").cast("double"))
    )
    
    logger.info(f'Starting save data in gs://{bucket}/{warehouse}/{destiny_table}')
    (source_df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true') \
        .save(f'gs://{bucket}/{warehouse}/{destiny_table}')
    )

    spark_session.stop()

def pipeline_cleaned_covid_with_filter(
    spark_session: SparkSession,
    warehouse: str,
    bucket: str,
    source_data: str,
    destiny_table: str,
    have_datafrag: bool,
    filter: str,
    datafrag_keysapce: str,
    datafrag_metatable: str,
    datafrag_warehouse: str
) -> None:
    logger.info(f'Read source data {source_data}')
    source_df = (spark_session.read
        .format('delta')
        .load(source_data)
    )
    
    source_df.printSchema()

    transformed_df = (source_df
        .groupby('Country_Region', 'Last_Update')
        .sum('Confirmed','Deaths','Recovered')
        .withColumnRenamed('sum(Deaths)', 'Deaths')
        .withColumnRenamed('sum(Confirmed)', 'Confirmed')
        .withColumnRenamed('sum(Recovered)','Recovered')
    )

    transformed_df.show(50)

    if have_datafrag:
        dfsAPI = datafragSparkAPI(spark_session, datafrag_keysapce, datafrag_metatable, datafrag_warehouse)
        dfsAPI.put_datafrag('covid_agg_data', transformed_df)

    logger.debug(f'filter: {filter}')

    transformed_df = transformed_df.where(filter)
    logger.info(f'Starting save data in gs://{bucket}/{warehouse}/{destiny_table}')
    (transformed_df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true') \
        .save(f'gs://{bucket}/{warehouse}/{destiny_table}')
    )

    spark_session.stop()

def pipeline_enrich_cleaned_covid_data(
    spark_session: SparkSession,
    warehouse: str,
    bucket: str,
    source_data: str,
    destiny_table: str,
    have_datafrag: bool,
    filter: str,
    datafrag_keysapce: str,
    datafrag_metatable: str,
    datafrag_warehouse: str
) -> None:
    logger.info(f'Read source data {source_data}')
    source_df = (spark_session.read
        .format('delta')
        .load(source_data)
    )
    
    transformed_df = (source_df
        .groupby('Country_Region', 'Last_Update')
        .sum('Confirmed','Deaths','Recovered')
        .withColumnRenamed('sum(Deaths)', 'Deaths')
        .withColumnRenamed('sum(Confirmed)', 'Confirmed')
        .withColumnRenamed('sum(Recovered)','Recovered')
    )
    if have_datafrag:
        dfsAPI = datafragSparkAPI(spark_session, datafrag_keysapce, datafrag_metatable, datafrag_warehouse)
        dfsAPI.put_datafrag('covid_agg_data',transformed_df)

    transformed_df = transformed_df.where(filter)
    logger.info(f'Starting save data in gs://{bucket}/{warehouse}/{destiny_table}')
    (transformed_df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true') \
        .save(f'gs://{bucket}/{warehouse}/{destiny_table}')
    )

    spark_session.stop()
