from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
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
    "nomis-transformations",
    default_args= task_args,
    description= "Process and curate NOMIS data for Anvil replacement",
    start_date= datetime.now(),
    schedule_interval= None,
    #start_date= datetime(2019, 2, 15),
    #schedule_interval= '0 2 * * *',
    catchup= False,
)

json_path = os.path.dirname(__file__) + "/dag_configs/nomis_transform_tasks.json"
with open(json_path) as f:
    airflow_tasks = json.load(f)


def assign_task_list_to_dag(target_dag, dag_config):

    # Define docker image and the AWS role (based on the airflow-repo)
    repo_name = "airflow-nomis-transform"
    repo_release_tag = "v2.0.17"
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


    # Define the set of tasks using the dag_config dictionary
    task_dic = dict()
    for tsk in dag_config["tasks"]:

        nom = f'nomis-{tsk["operation"]}-{tsk["task_id"]}'.replace("_","-")
        table_set_string = ','.join(t for t in tsk["table_set"])

        if "tsk_denorm" in tsk["task_id"]:
            s3_source = curate_source
        else:
            s3_source = process_source

        task_dic[tsk["task_id"]] = KubernetesPodOperator(
            dag= target_dag,
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
            labels= {"anvil": target_dag.dag_id},
            name= nom,
            in_cluster= True,
            task_id= nom,
            get_logs= True,
            annotations= {"iam.amazonaws.com/role": ROLE},
            )
    
    return task_dic


def set_task_dependencies(task_dic, dag_config):
    
    # Define the DAG dependencies using the dag_config dictionary
    for tsk in dag_config["tasks"]:
        for dep in tsk["task_dependency_ids"]:

            task_dic[dep] >> task_dic[tsk["task_id"]]
    
    return task_dic


task_dic = assign_task_list_to_dag(dag, airflow_tasks)
task_dic = set_task_dependencies(task_dic, airflow_tasks)
