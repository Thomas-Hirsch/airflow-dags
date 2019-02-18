from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago
import json
import os

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "mandarinduck",
    "email": ["adam.booker@digital.justice.gov.uk","anvil@noms.gsi.gov.uk"],
}

dag = DAG(
    "nomis-transform-seq-test",
    default_args= task_args,
    description= "NOMIS dependency data pipeline",
    #start_date= datetime.now(),
    #schedule_interval= None
    start_date= datetime(2019, 2, 15, 2),
    schedule_interval= timedelta(days= 1),
    catchup= False
)

#####
## NOMIS data transformations
#from nomis_transform import assign_task_list_to_dag as nomis_transform_tasks
#nomis_tsk_dic = nomis_transform_tasks(dag)

############### FULL NOMIS CODE

# Define docker image and the AWS role (based on the airflow-repo)
repo_name = "airflow-nomis-transform"
repo_release_tag = "v2.0.15"
IMAGE = f"593291632749.dkr.ecr.eu-west-1.amazonaws.com/{repo_name}:{repo_release_tag}"
ROLE = "airflow_nomis_transform"

process_source = "mojap-raw-hist/hmpps/nomis_t62"
destination = "alpha-anvil/curated"
curate_source = "alpha-anvil/curated"
athena_database = "anvil_beta"
db_ver = "v1"
gluejob_bucket = "alpha-nomis-discovery"
gluejob_role = ROLE
entry_py_script = "run.py"
work_capacity = "4"

json_path = os.path.dirname(__file__) + "/dag_configs/nomis_transform_tasks.json"
with open(json_path) as f:
    airflow_tasks = json.load(f)


# Define the set of tasks using the airflow_tasks .json file
task_dic = dict()
for tsk in airflow_tasks["tasks"]:

    nom = f'nomis-{tsk["operation"]}-{tsk["task_id"]}'.replace("_","-")
    table_set_string = ','.join(t for t in tsk["table_set"])

    if "tsk_denorm" in tsk["task_id"]:
        s3_source = curate_source
    else:
        s3_source = process_source

    task_dic[tsk["task_id"]] = KubernetesPodOperator(
        dag= dag,
        namespace= "airflow",
        image= IMAGE,
        env_vars= {
            "TABLES": table_set_string,
            "NOMIS_TRANSFORM": tsk["operation"],
            "SOURCE": s3_source,
            "DESTINATION": destination,
            "ATHENA_DB": athena_database,
            "DB_VERSION": db_ver,
            "PYTHON_SCRIPT_NAME": entry_py_script,
            "GLUE_JOB_BUCKET": gluejob_bucket,
            "GLUE_JOB_ROLE": gluejob_role,
            "ALLOCATED_CAPACITY": work_capacity,
            "AWS_METADATA_SERVICE_TIMEOUT": "60",
            "AWS_METADATA_SERVICE_NUM_ATTEMPTS": "5"
        },
        labels= {"anvil": dag.dag_id},
        name= nom,
        in_cluster= True,
        task_id= nom,
        get_logs= True,
        annotations= {"iam.amazonaws.com/role": ROLE},
        )


# Define the DAG dependencies using airflow_tasks.json
for tsk in airflow_tasks["tasks"]:
    for dep in tsk["task_dependency_ids"]:

        task_dic[dep] >> task_dic[tsk["task_id"]]


############### END FULL NOMIS CODE

#####
## VIPER task
#from viper import assign_task_to_dag as viper_task
#viper_tsk = viper_task(dag)

#nomis_tsk_dic["tsk_denorm_pop"] >> viper_tsk
#nomis_tsk_dic["tsk_denorm_inc_invol"] >> viper_tsk

#####
## Assault Reasons task
from airflow_assaults_reasons import assign_task_to_dag as ar_task
assault_reason_tsk = ar_task(dag)

task_dic["tsk_denorm_inc_invol"] >> assault_reason_tsk
task_dic["tsk_locations"] >> assault_reason_tsk
