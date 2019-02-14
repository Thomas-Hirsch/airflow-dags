from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

repo_name = "airflow-viper"
repo_release_tag = "v0.1.8"
VIPER_IMAGE = f"593291632749.dkr.ecr.eu-west-1.amazonaws.com/{repo_name}:{repo_release_tag}"
VIPER_ROLE = "airflow_nomis_viper"

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    #"retries": 3,
    #"retry_delay": timedelta(seconds=30),
    #"retry_exponential_backoff": True,
    "owner": "mandarinduck",
    "email": ["adam.booker@digital.justice.gov.uk","anvil@noms.gsi.gov.uk"],
}

viper_dag = DAG(
    "viper",
    default_args= task_args,
    description= "Runs the VIPER routine",
    start_date= datetime.now(),
    schedule_interval= None
    #start_date= datetime(2019, 1, 30),
    #schedule_interval= timedelta(days=1)
)

viper_task = KubernetesPodOperator(
        dag= viper_dag,
        namespace= "airflow",
        image= VIPER_IMAGE,
        env_vars= {
            "DATABASE": "anvil_beta",
            "OUTPUT_LOC": "alpha-anvil/curated",
            "AWS_DEFAULT_REGION": "eu-west-1"
        },
        labels= {"viper": viper_dag.dag_id},
        name= "viper",
        in_cluster= True,
        task_id= "viper",
        get_logs= True,
        startup_timeout_seconds= 500,
        annotations= {"iam.amazonaws.com/role": VIPER_ROLE},
        tolerations=[
            {
                "effect": "NoSchedule",
                "key": "dedicated",
                "operator": "Equal",
                "value": "highmem",
            },
            {
                "effect": "NoExecute",
                "key": "node.kubernetes.io/not-ready",
                "operator": "Exists",
                "tolerationSeconds": 300,
            },
            {
                "effect": "NoExecute",
                "key": "node.kubernetes.io/unreachable",
                "operator": "Exists",
                "tolerationSeconds": 300,
            }
        ],
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "node-role.kubernetes.io/highmem",
                                    "operator": "Exists",
                                }
                            ]
                        }
                    ]
                }
            }
        }
        )
