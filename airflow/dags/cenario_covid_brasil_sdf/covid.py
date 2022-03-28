from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

CONFIG = Variable.get('config_brazil', deserialize_json=True)

dag = DAG(
    'cenario_covid_brasil_sem_data_fragment',
    default_args={'max_active_runs': 1},
    description='submit pipeline of covid as sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    user_defined_macros={
        'env_config': CONFIG
    }
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

(covid_raw >> covid_raw_sensor >> cleaned_and_agg_covid >> cleaned_and_agg_covid_sensor)