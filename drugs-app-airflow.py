from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Define your docker image and the AWS role that will run the image (based on your airflow-repo)
IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-drugs-app:v0.0.11"
ROLE = "airflow_drugs_app"

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "meganstodel",
    "email": ["megan.stodel@justice.gov.uk"],
}

# # # Define your DAG
dag = DAG(
    "drugs_app",
    default_args=task_args,
    description="Check s3 for new drug finds data, then add to database if present.",
    #start_date= datetime.now(),
    #schedule_interval= None,
    start_date= datetime(2019, 3, 7),
    schedule_interval= '0 2 * * *',
    catchup=False
)

task_id = "drugs-app-data-update"
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
)
