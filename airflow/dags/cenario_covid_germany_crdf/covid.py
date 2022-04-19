from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from datafrag_manager import datafragOperationsAPI

CONFIG = Variable.get('config_germany', deserialize_json=True)

dag = DAG(
    'cenario_covid_germany_com_data_fragment',
    default_args={'max_active_runs': 1},
    description='submit pipeline of covid as sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    user_defined_macros={
        'env_config': CONFIG
    }
)

def get_cassandra_conn():
    auth_provider = PlainTextAuthProvider(username=CONFIG.get('cassandra_user'), password=CONFIG.get('cassandra_password'))
    cluster = Cluster([CONFIG.get('cassandra_host')], auth_provider=auth_provider, port=CONFIG.get('cassandra_port'))
    return cluster.connect()

def with_datafragment(datafrag: str):
    dfoAPI = datafragOperationsAPI(
        cassandra_connection=get_cassandra_conn(),
        keyspace=CONFIG.get('datafrag_keyspace'),
        table=CONFIG.get('datafrag_metatable')
    )
    if dfoAPI.have_datafrag_operation(datafrag):
        return 'cleaned_and_agg_covid_df_submit'
    else:
        return 'covid_raw_submit'

way_of_pipeline = BranchPythonOperator(
    task_id='way_of_pipeline',
    python_callable=with_datafragment,
    op_args=['covid_agg_data']
)

covid_raw = SparkKubernetesOperator(
    task_id='covid_raw_submit',
    namespace="default",
    application_file="covid_raw.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

covid_raw_sensor = SparkKubernetesSensor(
    task_id='covid_raw_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='covid_raw_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

cleaned_and_agg_covid = SparkKubernetesOperator(
    task_id='cleaned_and_agg_covid_submit',
    namespace="default",
    application_file="cleaned_and_agg_covid.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

cleaned_and_agg_covid_sensor = SparkKubernetesSensor(
    task_id='cleaned_and_agg_covid_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='cleaned_and_agg_covid_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

cleaned_and_agg_covid_df = SparkKubernetesOperator(
    task_id='cleaned_and_agg_covid_df_submit',
    namespace="default",
    application_file="cleaned_and_agg_covid_df.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

cleaned_and_agg_covid_df_sensor = SparkKubernetesSensor(
    task_id='cleaned_and_agg_covid_df_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='cleaned_and_agg_covid_df_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

way_of_pipeline >> cleaned_and_agg_covid_df >> cleaned_and_agg_covid_df_sensor
way_of_pipeline >> covid_raw >> covid_raw_sensor >> cleaned_and_agg_covid >> cleaned_and_agg_covid_sensor
