from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

TAR_S3_RELATIVE_FOLDER_PATHS = ""
TAR_PROCESS_SCRIPT = "tar_raw_to_process.py"
TAR_DB_SCRIPT = "rebuild_tar_databases.py"
DB_VERSION = 'v1'

# HOCAS_RAW_FOLDER = "s3://mojap-raw/hmcts/hocas/"
# HOCAS_PYTHON_SCRIPT_NAME = "hocas_raw_to_process.py"

MAGS_RAW_TO_PROCESSED_IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-magistrates-data-engineering:v0.0.5"
MAGS_RAW_TO_PROCESSED_ROLE = "airflow_mags_data_processor"

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "isichei",
    "email": ["karik.isichei@digital.justice.gov.uk"],
}

# Catch-up on dates before today-REUPDATE_LAST_N_DAYS days
dag = DAG(
    "mags_data_raw_to_processed",
    default_args=task_args,
    description="Process mags data (HOCAS and TAR)",
    start_date=datetime.now(),
    schedule_interval=None,
)

tasks = {}

task_id = "rebuild-athena-schemas"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=MAGS_RAW_TO_PROCESSED_IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": TAR_DB_SCRIPT,
        "DB_VERSION": DB_VERSION
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": MAGS_RAW_TO_PROCESSED_ROLE},
)

task_id = "process-tar"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=MAGS_RAW_TO_PROCESSED_IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": TAR_PROCESS_SCRIPT,
        "S3_RELATIVE_FOLDER_PATHS": TAR_S3_RELATIVE_FOLDER_PATHS,
        "DB_VERSION": DB_VERSION
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": MAGS_RAW_TO_PROCESSED_ROLE},
)

# Set dependencies
tasks['process-tar'] >> tasks["rebuild-athena-schemas"]