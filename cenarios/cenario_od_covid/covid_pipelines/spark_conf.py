import os

from pyspark.sql import SparkSession


def setup_spark(
    app_name: str,
    cassandra_user: str,
    cassandra_password: str,
    cassandra_host: str = 'localhost',
    cassandra_port: str = '9042'
) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension,com.datastax.spark.connector.CassandraSparkExtensions')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.delta.logStore.gs.impl', 'io.delta.storage.GCSLogStore')
        .config('spark.cassandra.connection.host', cassandra_host)
        .config('spark.cassandra.connection.port', cassandra_port)
        # .config('spark.cassandra.auth.username', cassandra_user)
        # .config('spark.cassandra.auth.password', cassandra_password)
        .config('spark.cassandra.output.consistency.level', 'LOCAL_ONE')
        .getOrCreate()
    )

    return spark

def setup_spark_only_cassandra(
    app_name: str,
    cassandra_user: str,
    cassandra_password: str,
    cassandra_host: str = 'localhost',
    cassandra_port: str = '9042'
) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions')
        .config('spark.cassandra.connection.host', cassandra_host)
        .config('spark.cassandra.connection.port', cassandra_port)
        # .config('spark.cassandra.auth.username', cassandra_user)
        # .config('spark.cassandra.auth.password', cassandra_password)
        .config('spark.cassandra.output.consistency.level', 'LOCAL_ONE')
        .getOrCreate()
    )

    return spark

ENV_CONFIG = {
    'warehouse': os.getenv('WAREHOUSE'),
    'cassandra_user': os.getenv('CASSANDRA_USER'),
    'cassandra_password': os.getenv('CASSANDRA_PASSWORD'),
    'cassandra_host': os.getenv('CASSANDRA_HOST'),
    'cassandra_port': os.getenv('CASSANDRA_PORT'),
    'cassandra_user': os.getenv('CASSANDRA_USER'),
    'google_bucket': os.getenv('GOOGLE_BUCKET'),
    'datafrag_warehouse': os.getenv('DATAFRAG_WAREHOUSE'),
    'datafrag_metatable': os.getenv('DATAFRAG_METATABLE'),
    'datafrag_tc_metatable': os.getenv('DATAFRAG_TC_METATABLE'),
    'datafrag_keyspace': os.getenv('DATAFRAG_KEYSPACE'),
}