from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import (
    KubernetesPodOperator
)


IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-viper-to-external:v0.0.00"
ROLE = "airflow_viper_to_external"

INTERNAL_BUCKET = "alpha-anvil"
AIRFLOW_JOB_NAME = "viper_external"
EXTERNAL_BUCKET = "external-bucket-name"

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "s-block",
    "email": ["josh.rowe@digital.justice.gov.uk"],
}

dag = DAG(
    AIRFLOW_JOB_NAME,
    default_args=task_args,
    description="Copy viper extract to external bucket",
    start_date=datetime(2019, 3, 18),
    schedule_interval="0 */2 * * *",
)

tasks = {}

task_id = "viper-external"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "INTERNAL_BUCKET": INTERNAL_BUCKET,
        "EXTERNAL_BUCKET": EXTERNAL_BUCKET,
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
)
