from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# GLOBAL ENV VARIABLES
IMAGE_VERSION = "v0.0.17"
IMAGE = f"593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-psst-data:{IMAGE_VERSION}"
ROLE = "airflow_psst_data"

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "samtazzyman",
    "email": ["samuel.tazzyman@digital.justice.gov.uk"],
}

# DAG defined
dag = DAG(
    "psst_data",
    default_args=task_args,
    description="get new prison reports, process them, and put them in the psst",
    start_date=datetime(2019, 2, 21),
    schedule_interval="@daily",
    catchup=False,
)

task_id = "psst-data"
task1 = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    startup_timeout_seconds=500,
    annotations={"iam.amazonaws.com/role": ROLE},
)