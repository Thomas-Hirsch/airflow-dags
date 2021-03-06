from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "mandarinduck",
    "email": ["adam.booker@digital.justice.gov.uk","anvil@noms.gsi.gov.uk"],
    "retries": 5,
    "retry_delay": timedelta(seconds=60),
    "retry_exponential_backoff": True,
}

dag = DAG(
    "nomis-transform-seq-test",
    default_args= task_args,
    description= "NOMIS dependency data pipeline",
    start_date= datetime(2019, 2, 20),
    schedule_interval= '0 2 * * *',
    catchup= False
)

#####
## NOMIS data transformations
from nomis_transform import airflow_tasks as nomis_config
from nomis_transform import assign_task_list_to_dag as nomis_transform_tasks_assign
from nomis_transform import set_task_dependencies as set_nomis_dependencies
nomis_tsk_dic = nomis_transform_tasks_assign(dag, nomis_config)
nomis_tsk_dic = set_nomis_dependencies(nomis_tsk_dic, nomis_config)


#####
## VIPER task
from viper import assign_task_to_dag as viper_task_assign
viper_tsk = viper_task_assign(dag)

nomis_tsk_dic["tsk_denorm_pop"] >> viper_tsk
nomis_tsk_dic["tsk_denorm_inc_invol"] >> viper_tsk


#####
## Assault Reasons task
from airflow_assaults_reasons import assign_task_to_dag as ar_task_assign
assault_reason_tsk = ar_task_assign(dag)

nomis_tsk_dic["tsk_denorm_inc_invol"] >> assault_reason_tsk
nomis_tsk_dic["tsk_locations"] >> assault_reason_tsk


#####
## SDT dashboard task
from sdt_airflow import assign_task_to_dag as sdt_task_assign
sdt_task = sdt_task_assign(dag)

viper_tsk >> sdt_task
assault_reason_tsk >> sdt_task
nomis_tsk_dic["tsk_denorm_pop"] >> sdt_task
nomis_tsk_dic["tsk_offender_attr"] >> sdt_task
nomis_tsk_dic["tsk_denorm_inc_invol"] >> sdt_task
nomis_tsk_dic["tsk_offender_core"] >> sdt_task #redundant as tsk_denorm_inc_invol an interim dependency
nomis_tsk_dic["tsk_incidents"] >> sdt_task #redundant as tsk_denorm_inc_invol an interim dependency