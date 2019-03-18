from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Define your docker image and the AWS role that will run the image (based on your airflow-repo)
IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-bentham-app:v0.3.4"
ROLE = "airflow_bentham_app"

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "owner": "joeprinold",
    "email": ["joe.prinold@digital.justice.gov.uk"],
}

# # # Define your DAG
dag = DAG(
    "bentham_app_2",
    default_args=task_args,
    description="Check s3 for new phone data, then add to database if present.",
    start_date=datetime(2019, 1, 6, 2),
    schedule_interval='0 1 * * *',
    catchup=False
)

task_id = "bentham-app-data-update"
task1 = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
    env_vars={"AWS_DEFAULT_REGION": "eu-west-1"}
)
