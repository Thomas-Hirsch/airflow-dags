from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import datetime, timedelta

log = LoggingMixin().log

try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "Thomas-hirsch",
            "start_date": datetime(2019, 2, 11),
            "email": ["thomas.hirsch@justice.gov.uk"],
            "pool": "occupeye_pool"}

    dag = DAG(
        dag_id="occupeye_aggregator",
        default_args=args,
        schedule_interval='0 5 * * *',
    )

    surveys_to_s3 = KubernetesPodOperator(
        namespace="airflow",
        image="593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-occupeye-dashboard-aggregation:v0.13",
        env_vars={
            "AWS_METADATA_SERVICE_TIMEOUT": "60",
            "AWS_METADATA_SERVICE_NUM_ATTEMPTS": "5"
        },
        cmds=["bash", "-c"],
        arguments=["Rscript main.R"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="scrape_all",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "airflow_occupeye_aggregator"},
        image_pull_policy='Always'
    )



except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))
