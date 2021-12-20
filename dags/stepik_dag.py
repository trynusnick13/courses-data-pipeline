import os
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago
import yaml


dag_name = Path(__file__).stem
config_path = os.path.join(os.path.dirname(__file__), "ingestion-config.yml")

with open(config_path) as configs:
    ingestion_config = yaml.safe_load(configs)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "email": "trynuspoc@gmail.com",
    "retries": 0,
    "retry_delay": timedelta(seconds=60),
}

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    description="Scrape courses from stepik and load it to BQ and Heroku postgres",
    schedule_interval="@daily",
)

stepik_to_gcs = KubernetesPodOperator(
    namespace='default',
    dag=dag,
    name='run_stepik_ingestion',
    task_id="stepic_to_gcs",
    image_pull_policy="IfNotPresent",
    is_delete_operator_pod=True,
    image=ingestion_config.get("ingestion_image"),
    resources={
        'request_memory': '128Mi',
        'request_cpu': '500m',
        'limit_memory': '500Mi',
        'limit_cpu': 1
    },
)

gcs_to_postgres = KubernetesPodOperator(
    namespace='default',
    dag=dag,
    name='merge_to_postgres',
    task_id="gcs_to_postgres",
    image_pull_policy="IfNotPresent",
    is_delete_operator_pod=True,
    image=ingestion_config.get("gcs_to_postgres_image"),
    resources={
        'request_memory': '128Mi',
        'request_cpu': '500m',
        'limit_memory': '500Mi',
        'limit_cpu': 1
    },
)

great_expectations_stepik = KubernetesPodOperator(
    namespace='default',
    dag=dag,
    name='great_expectations_stepik',
    task_id="great_expectations_stepik",
    image_pull_policy="IfNotPresent",
    # is_delete_operator_pod=True,
    image=ingestion_config.get("great_expectations_image"),
    # resources={
    #     'request_memory': '128Mi',
    #     'request_cpu': '500m',
    #     'limit_memory': '500Mi',
    #     'limit_cpu': 1
    # },
)

gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="gcs_to_bq",
    bucket=ingestion_config.get("bucket"),
    source_objects=['stepik/*'],
    destination_project_dataset_table=f"{ingestion_config.get('destination_dataset')}.{ingestion_config.get('desination_stepik_table')}",
    schema_fields=[
        {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'authors', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'link', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'image', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'rating', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'created_ts', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'created_by', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

stepik_to_gcs >> [gcs_to_postgres, gcs_to_bq]
gcs_to_bq >> great_expectations_stepik
