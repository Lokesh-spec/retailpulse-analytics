import logging
from datetime import timedelta
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator

from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode

from plugins.tasks.notify import notify_failure

# Retrieve Airflow Variables
MY_BUCKET = Variable.get('GCS_BUCKET')
PROJECT_ID = Variable.get('PROJECT_ID')
BQ_RAW_TABLE = Variable.get('bq_raw_dataset_id')

# Logger
logger = logging.getLogger(__name__)


@dag(
    start_date=days_ago(1),
    max_active_runs=2,
    schedule=None,
    catchup=False,
    tags=['bi', 'bigquery', 'dbt'],
    description="Load products CSV from GCS to BigQuery",
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': notify_failure
    }
)
def load_products_to_bq_dbt():

    # Task: Log info about the triggered DAG run
    def log_run_info(**kwargs):
        run_id = kwargs['dag_run'].run_id
        gcs_path = kwargs['dag_run'].conf.get('gcs_path', None)
        logger.info(f"Triggered DAG run id: {run_id}")
        logger.info(f"GCS path: {gcs_path}")

    log_info = PythonOperator(
        task_id='log_run_info',
        python_callable=log_run_info,
        provide_context=True
    )

    # Task: Load CSV from GCS to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket=MY_BUCKET,
        source_objects="{{ dag_run.conf['gcs_path'] }}",
        destination_project_dataset_table=BQ_RAW_TABLE,
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        field_delimiter=",",
        encoding="UTF-8",
        autodetect=True,
        gcp_conn_id="google_cloud_default",
        project_id=PROJECT_ID,
    )

    dbt_task_group = DbtTaskGroup(
        group_id="products_dbt",

        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/include/dbt/retailpulse_dbt",
        ),

        profile_config=ProfileConfig(
            profile_name="retailpulse_dbt",
            target_name="dev",
            profiles_yml_filepath="/usr/local/airflow/include/dbt/retailpulse_dbt/profiles.yml"
        ),

        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL
        ),

        render_config=RenderConfig(
            select=[
                "tag:products",
                "tag:inventory",
                "tag:dimension",
                "tag:fact"
            ]
        ),

        operator_args={
            "install_deps": True
        }
    )

    # Set dependencies
    log_info >> load_to_bq >> dbt_task_group


# Instantiate the DAG
load_products_to_bq_dbt_dag = load_products_to_bq_dbt()
