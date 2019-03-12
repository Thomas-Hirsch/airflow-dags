from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import datetime, timedelta

log = LoggingMixin().log

SCRAPER_IMAGE = "quay.io/mojanalytics/airflow-elasticsearch-kpis:latest"
SCRAPER_IAM_ROLE = "airflow_platform_kpis"

try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "David",
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
            "email_on_failure": True,
            "email_on_retry": False,
            "email": ["david.read@digital.justice.gov.uk"]}

    dag = DAG(
        dag_id="platform_kpi_scraper",
        default_args=args,
        schedule_interval='@hourly',
        catchup=False,
    )
    # https://github.com/apache/incubator-airflow/blob/5a3f39913739998ca2e9a17d0f1d10fccb840d36/airflow/contrib/operators/kubernetes_pod_operator.py#L129
    surveys_to_s3 = KubernetesPodOperator(
        namespace="airflow",
        image=SCRAPER_IMAGE,
        image_pull_policy='Always',
        cmds=["bash", "-c"],
        arguments=["python main.py"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="get_kpis",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": SCRAPER_IAM_ROLE},
    )

except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))
