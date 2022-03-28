import datetime

import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from datafrag_manager.datafrag_manager import datafragSparkAPI

def test_no_instance_datafragSparkAPI():
    with pytest.raises(Exception):
        datafragSparkAPI()

def test_instance_datafragSparkAPI():
    spark_session = SparkSession.builder.appName('test_instance_datafragSparkAPI').getOrCreate()
    keyspace = 'dataflow_operation'
    table = 'datafrag_operations'
    datafrag_warehouse = 'warehouse_frag'
    instance = datafragSparkAPI(spark_session, keyspace, table, datafrag_warehouse)
    assert type(instance) is datafragSparkAPI

def test_datafragSparkAPI_extract_metadata():
    
    spark_session = SparkSession.builder.appName('test_datafragSparkAPI_extract_metadata').getOrCreate()
    keyspace = 'dataflow_operation'
    table = 'datafrag_operations'
    datafrag_warehouse = 'warehouse_frag'
    df = spark_session.createDataFrame(
        [
            ['a', 4, 1.0],
            ['b', 7, 2.3],
            ['a', 1, 2.56],
            ['c', 8, 2.98],
        ],
        ['first_field', 'field_number', 'field_number2']
    )
    dfg = df.groupby("first_field").sum("field_number").withColumnRenamed("sum(field_number)", "sum_field_number")
    dfsapi = datafragSparkAPI(spark_session, keyspace, table, datafrag_warehouse)
    actual = dfsapi._extract_metadata('test_frag', dfg)
    expected = {
        'dataflow': 'test_frag',
        'operation': {
            'logicalrdd': {'value': '[first_field#0, field_number#1L, field_number2#2], false', 'cost': '8.0 EiB'},
            'project': {'value': '[first_field#0, field_number#1L]', 'cost': '6.5 EiB'},
            'aggregate': {'value': '[first_field#0], [first_field#0, sum(field_number#1L) AS sum_field_number#13L]', 'cost': '6.5 EiB'}
            },
        'timestamp': datetime.datetime.now().replace(microsecond=0)
    }
    assert expected == actual

def test_datafragSparkAPI_exception_extract_metadata():
    spark_session = SparkSession.builder.appName('test_datafragSparkAPI_exception_extract_metadata').getOrCreate()
    keyspace = 'dataflow_operation'
    table = 'datafrag_operations'
    datafrag_warehouse = 'warehouse_frag'
    dfsapi = datafragSparkAPI(spark_session, keyspace, table, datafrag_warehouse)
    with pytest.raises(Exception):
        dfsapi._extract_metadata('df_test_extract')

def test_datafragSparkAPI_exception_schema_operation():
    spark_session = SparkSession.builder.appName('test_datafragSparkAPI_exception_schema_operation').getOrCreate()
    keyspace = 'dataflow_operation'
    table = 'datafrag_operations'
    datafrag_warehouse = 'warehouse_frag'
    dfsapi = datafragSparkAPI(spark_session, keyspace, table, datafrag_warehouse)
    actual = dfsapi._schema_operation()
    expected = StructType([
        StructField("dataflow", StringType()),
        StructField("operation", StringType()),
        StructField("timestamp", TimestampType())
    ])
    assert expected == actual

    