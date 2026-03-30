import time
import json
import logging
import re
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from plugins.tasks.product_api import fetch_product_data_api
from plugins.tasks.flatten_api_response import flatten_api_data
from plugins.tasks.notify import notify_failure

logger = logging.getLogger(__name__)

# Config
BUCKET_NAME = Variable.get("GCS_BUCKET", default_var="retailpulse-raw-data")
API_CALL_COUNT = int(Variable.get("API_CALL_COUNT", default_var=5))
API_TIMEOUT = int(Variable.get("API_TIMEOUT", default_var=10))


@dag(
    start_date=days_ago(1),
    schedule='*/10 * * * *',  
    description="DAG to fetch product data from API, transform, and upload to GCS",
    catchup=False,
    max_active_runs=2,       
    tags=['api', 'product', 'extract'],
    default_args={
        'retries': 1,       
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': notify_failure 
    }
)
def product_data_dag():

    @task()
    def _extract() -> str:
        conn = BaseHook.get_connection('datamock_api')
        url = f"{conn.host}/v1/product"
        headers = {'Accept': 'application/json'}

        all_products = []

        for i in range(API_CALL_COUNT):
            data = fetch_product_data_api(url, headers, timeout=API_TIMEOUT)

            if data:
                message = data.get('meta', {}).get('message', 'No message')
                logger.info(f"Fetched message: {message}")

                products = data.get('data', [])
                logger.info(f"Products fetched: {len(products)}")

                if products:
                    logger.info(f"First SKU: {products[0].get('sku')}")
            else:
                logger.warning("No data fetched")
            
            if data:
                all_products.append(data)

            time.sleep(1)

        file_path = "/tmp/raw_products.json"
        with open(file_path, "w") as f:
            json.dump(all_products, f)

        return file_path


    @task()
    def _transform(file_path: str) -> str:
        with open(file_path, "r") as f:
            data = json.load(f)

        df = flatten_api_data(data)

        output_path = "/tmp/raw_products.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to {output_path}")

        return output_path
    
    @task()
    def _upload_to_gcs(local_path: str) -> dict:
        context = get_current_context()
        run_id = context["run_id"]
        # Sanitize run_id: only allow alphanumeric, dash, dot, underscore
        sanitized_run_id = re.sub(r"[^a-zA-Z0-9._-]", "_", run_id)
        gcs_path = f"products/{sanitized_run_id}/Product.csv"
        
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=gcs_path,
            filename=local_path
        )

        return {
            "run_id": sanitized_run_id,
            "gcs_path": gcs_path
        }

    # Define tasks
    extract = _extract()
    transform = _transform(extract)
    upload = _upload_to_gcs(transform)

    # Trigger downstream DAG using TriggerDagRunOperator directly
    trigger_downstream = TriggerDagRunOperator(
        task_id="trigger_load_products_to_bq_dbt",
        trigger_dag_id="load_products_to_bq_dbt",
        conf={
            "dag_run_id": "{{ task_instance.xcom_pull(task_ids='_upload_to_gcs')['run_id'] }}",
            "gcs_path": "{{ task_instance.xcom_pull(task_ids='_upload_to_gcs')['gcs_path'] }}"
        },
        wait_for_completion=False
    )

    # Set dependencies
    extract >> transform >> upload >> trigger_downstream


product_data_dag()