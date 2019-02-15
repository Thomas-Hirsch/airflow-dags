from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "filippoberio",
    "email": ["philip.dent2@digital.justice.gov.uk"],
}

# # # Define your DAG
# Some notes:
# setting - (start_date=datetime.now() and schedule_interval=None) is a way to set up you tag so it can only be triggered manually
# To actually put it on a schedule you can set something like:
# start_date=datetime(2018, 8, 1), schedule_interval=timedelta(days=1)
dag = DAG(
    "assaults-reasons",
    default_args=task_args,
    description="run at a specified time of day",
    start_date=datetime(2019, 2, 1, 2),
    schedule_interval=timedelta(days=1),
    catchup=False
)

def assign_task_to_dag(target_dag):

    # Define your docker image and the AWS role that will run the image (based on your airflow-repo)
    IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-assault-reasons:v0.4.1"
    ROLE = "airflow_assaults_reasons"
    task_id = "assaults-reasons-data-update"

    return KubernetesPodOperator(
        dag= target_dag,
        namespace="airflow",
        image=IMAGE,
        labels={"app": target_dag.dag_id},
        name=task_id,
        in_cluster=True,
        task_id=task_id,
        get_logs=True,
        annotations={"iam.amazonaws.com/role": ROLE},
    )

task1 = assign_task_to_dag(dag)