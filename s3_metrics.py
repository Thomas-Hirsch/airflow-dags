from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import (
    KubernetesPodOperator
)


IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-s3-metrics:v0.0.4"
ROLE = "airflow_s3_metrics"

AWS_S3_BUCKET = "alpha-s3-metrics"
AIRFLOW_JOB_NAME = "s3_metrics"

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "s-block",
    "email": ["josh.rowe@digital.justice.gov.uk"],
}

dag = DAG(
    "s3_metrics",
    default_args=task_args,
    description="Download s3 metrics from cloudwatch and add them to a bucket",
    start_date=datetime(2019, 2, 25),
    schedule_interval="0 22 * * *",
)

tasks = {}

task_id = "s3-metrics"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "AWS_S3_BUCKET": AWS_S3_BUCKET,
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
)
