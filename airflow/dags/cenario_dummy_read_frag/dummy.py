from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from pyspark.sql import SparkSession

from datafrag_manager import datafragSparkAPI, get_metadata_table_representation

CONFIG = Variable.get('config_dummy', deserialize_json=True)

def get_spark_conn():
    spark = (
        SparkSession.builder.appName('cenario_dummy_test_data_containment')
        .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.2.0')
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
        dataflow='containment_dummy_filter',
        task=__name__, 
        datasource='gs://lncc-tese-datafrag/dummy_warehouse/dummy_raw', 
        filter=filter
    ) 
    containment = dfsAPI.resolve_containment(
        df_consumer=df_consumer
    )

    return containment.get('contain'), containment.get('dataflow') 

def with_datafragment(with_frag: bool):
    if with_frag:
        return 'cleaned_and_agg_covid_df'
    else:
        return 'covid_raw'

with_frag, dataflow = get_info_datafragment('((A#1 >= 6 AND A#1 <= 8) AND ((B#2 = 6) OR (C#3 >= 11 AND C#3 <= 13)))')

dag = DAG(
    'cenario_dummy_read_data_containment',
    default_args={'max_active_runs': 1},
    description='submit pipeline of dummy as sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 21),
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

dummy_raw = SparkKubernetesOperator(
    task_id='dummy_raw_submit',
    namespace="default",
    application_file="dummy_raw.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

dummy_raw_sensor = SparkKubernetesSensor(
    task_id='dummy_raw_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='dummy_raw_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

cleaned_and_agg_dummy = SparkKubernetesOperator(
    task_id='cleaned_and_agg_dummy_submit',
    namespace="default",
    application_file="cleaned_and_agg_dummy.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

cleaned_and_agg_dummy_sensor = SparkKubernetesSensor(
    task_id='cleaned_and_agg_covid_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='cleaned_and_agg_dummy_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

cleaned_and_agg_dummy_df = SparkKubernetesOperator(
    task_id='cleaned_and_agg_dummy_df_submit',
    namespace="default",
    application_file="cleaned_and_agg_dummy_df.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

cleaned_and_agg_dummy_df_sensor = SparkKubernetesSensor(
    task_id='cleaned_and_agg_dummy_df_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='cleaned_and_agg_dummy_df_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

way_of_pipeline >> cleaned_and_agg_dummy_df >> cleaned_and_agg_dummy_df_sensor
way_of_pipeline >> dummy_raw >> dummy_raw_sensor >> cleaned_and_agg_dummy >> cleaned_and_agg_dummy_sensor
