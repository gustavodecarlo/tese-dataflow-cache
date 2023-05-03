from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from pyspark.sql import SparkSession

from datafrag_manager import datafragSparkAPI, get_metadata_table_representation

CONFIG = Variable.get('config_nycytaxi_ns_vfi', deserialize_json=True)

def get_spark_conn():
    spark = (
        SparkSession.builder.appName('cenario_nyc_taxi_vfi_containment')
        .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.3.0')
        .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions')
        .config('spark.cassandra.connection.host', CONFIG.get('cassandra_host'))
        .config('spark.cassandra.connection.port', CONFIG.get('cassandra_port'))
        .config('spark.cassandra.output.consistency.level', 'LOCAL_ONE')
        .getOrCreate()
    )

    return spark

def get_info_datafragment(filter: str):
    spark_session = get_spark_conn()
    dfsAPI = datafragSparkAPI(
        spark_session=spark_session,
        keyspace=CONFIG.get('datafrag_keyspace'),
        table=CONFIG.get('datafrag_metatable'),
        table_containment=CONFIG.get('datafrag_tc_metatable'),
        datafrag_warehouse=CONFIG.get('datafrag_warehouse')
    )
    df_consumer = get_metadata_table_representation(
        spark_session=spark_session,
        dataflow='containment_filter',
        task=__name__, 
        datasource='nyc_yellow_taxi', 
        filter=filter
    )
    try:
        containment = dfsAPI.resolve_containment(
            df_consumer=df_consumer
        )
    except Exception as e:
        print(e)
        return None, None

    return containment.get('contain'), containment.get('dataflow') 

def with_datafragment(with_frag: bool):
    print(with_frag)
    if with_frag:
        return 'cleaned_and_agg_nycytaxi_df_submit'
    else:
        return 'nycytaxi_raw_submit'

with_frag, dataflow = get_info_datafragment('trip_distance#1 >= 2 AND passenger_count#2 >= 3')

dag = DAG(
    'cenario_nycytaxi_ns_read_data_containment',
    default_args={'max_active_runs': 1},
    description='submit pipeline of nycytaxi as sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 25),
    catchup=False,
    user_defined_macros={
        'env_config': CONFIG,
        'datafrag_config': {
            'dataflow': dataflow
        }
    }
)

way_of_pipeline = BranchPythonOperator(
    task_id='way_of_pipeline',
    python_callable=with_datafragment,
    op_args=[with_frag]
)

nycytaxi_raw = SparkKubernetesOperator(
    task_id='nycytaxi_raw_submit',
    namespace="default",
    application_file="nycytaxi_raw.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

nycytaxi_raw_sensor = SparkKubernetesSensor(
    task_id='nycytaxi_raw_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='nycytaxi_raw_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

cleaned_and_agg_nycytaxi = SparkKubernetesOperator(
    task_id='cleaned_and_agg_nycytaxi_submit',
    namespace="default",
    application_file="cleaned_and_agg_nycytaxi.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

cleaned_and_agg_nycytaxi_sensor = SparkKubernetesSensor(
    task_id='cleaned_and_agg_nycytaxi_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='cleaned_and_agg_nycytaxi_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

cleaned_and_agg_nycytaxi_df = SparkKubernetesOperator(
    task_id='cleaned_and_agg_nycytaxi_df_submit',
    namespace="default",
    application_file="cleaned_and_agg_nycytaxi_df.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

cleaned_and_agg_nycytaxi_df_sensor = SparkKubernetesSensor(
    task_id='cleaned_and_agg_nycytaxi_df_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='cleaned_and_agg_nycytaxi_df_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

way_of_pipeline >> cleaned_and_agg_nycytaxi_df >> cleaned_and_agg_nycytaxi_df_sensor
way_of_pipeline >> nycytaxi_raw >> nycytaxi_raw_sensor >> cleaned_and_agg_nycytaxi >> cleaned_and_agg_nycytaxi_sensor
