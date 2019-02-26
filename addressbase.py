

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


log = LoggingMixin().log

IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-build-addressbase-premium:v0.0.4"
ROLE = "airflow_addressbasepremium"


args = {"owner": "Robin",
        "retries":0,
        "email": ["robin.linacre@digital.justice.gov.uk"],
        "start_date": days_ago(1)}

dag = DAG(
    dag_id="build_addressbase",
    default_args=args,
    schedule_interval='@once',
)

run_job = KubernetesPodOperator(
    namespace="airflow",
    image=IMAGE,
    cmds=["bash", "-c"],
    arguments=["python main.py"],
    labels={"foo": "bar"},
    name="addressbase-pod-glue",
    in_cluster=True,
    task_id="build_addressbase_db",
    get_logs=True,
    dag=dag,
    annotations={"iam.amazonaws.com/role": ROLE},
    image_pull_policy='Always'
)


