from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "filippoberio",
    "email": ["philip.dent2@digital.justice.gov.uk"],
}

dag = DAG(
    "sdt",
    default_args=task_args,
    description="run at a specified time of day",
    start_date= datetime.now(),
    schedule_interval= None,
    #start_date=datetime(2018, 12, 19),
    #schedule_interval= '0 4 * * *',
    catchup=False
)

def assign_task_to_dag(target_dag):

    # Define your docker image and the AWS role that will run the image (based on your airflow-repo)
    IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-sdt:v1.6.3"
    ROLE = "airflow_sdt"
    task_id = "sdt-data-update"
    
    return KubernetesPodOperator(
        dag= target_dag,
        namespace="airflow",
        image=IMAGE,
        labels={"app": dag.dag_id},
        name=task_id,
        in_cluster=True,
        task_id=task_id,
        get_logs=True,
        annotations={"iam.amazonaws.com/role": ROLE},
    )

task = assign_task_to_dag(dag)
