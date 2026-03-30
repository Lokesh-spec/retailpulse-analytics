import logging
from airflow.utils.email import send_email
from airflow.models import Variable

logger = logging.getLogger(__name__)

ALERT_EMAIL = Variable.get('ALERT_EMAIL', default_var='lokeshkv18@gmail.com')


def notify_failure(context: dict) -> None:
    """Send email alert on task failure."""

    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    exception = context.get('exception')

    subject = f"Airflow Alert: Task {task_instance.task_id} Failed in DAG {dag_run.dag_id}"

    html_content = f"""
    <h3>Task Failure Alert</h3>
    <p><strong>DAG:</strong> {dag_run.dag_id}</p>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>Execution Time:</strong> {context.get('execution_date')}</p>
    <p><strong>Error:</strong> <span style="color:red;">{exception}</span></p>
    <p><strong>Log URL:</strong> <a href="{task_instance.log_url}">View Logs</a></p>
    """

    try:
        send_email(
            to=[ALERT_EMAIL],
            subject=subject,
            html_content=html_content,
            conn_id='smtp_default'
        )
        logger.info(
            f'Failure mail sent to {ALERT_EMAIL} for DAG={dag_run.dag_id}, Task={task_instance.task_id}')
    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
