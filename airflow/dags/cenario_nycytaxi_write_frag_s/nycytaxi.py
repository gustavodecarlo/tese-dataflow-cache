from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

CONFIG = Variable.get('config_nycytaxi_s_cmt', deserialize_json=True)

dag = DAG(
    'cenario_taxis_s_write_data_fragment',
    default_args={'max_active_runs': 1},
    description='submit pipeline of nycytaxi selective as sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 25),
    catchup=False,
    user_defined_macros={
        'env_config': CONFIG
    }
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

nycytaxi_cleaned_and_filter_containment = SparkKubernetesOperator(
    task_id='nycytaxi_cleaned_and_filter_containment_submit',
    namespace="default",
    application_file="nycytaxi_cleaned_and_filter_containment.yml",
    kubernetes_conn_id="k8s_config_conn",
    do_xcom_push=True,
    dag=dag,
)

nycytaxi_cleaned_and_filter_containment_sensor = SparkKubernetesSensor(
    task_id='nycytaxi_cleaned_and_filter_containment_sensor',
    namespace="default",
    kubernetes_conn_id="k8s_config_conn",
    application_name="{{ task_instance.xcom_pull(task_ids='nycytaxi_cleaned_and_filter_containment_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=False,
)

(nycytaxi_raw >> nycytaxi_raw_sensor >> nycytaxi_cleaned_and_filter_containment >> nycytaxi_cleaned_and_filter_containment_sensor)