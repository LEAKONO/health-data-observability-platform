from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract.cdc_covid_deaths_extractor import run_extraction as extract_covid
from src.extract.cdc_flu_extractor import run_extraction as extract_flu
from src.load.snowflake_loader import load_covid_deaths, load_flu_surveillance
from src.validation.validate_covid_deaths import run_validation as validate_covid
from src.validation.validate_flu_surveillance import run_validation as validate_flu
from src.observability.anomaly_detector import run_anomaly_detection
from src.observability.alerting import send_failure_alert

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_failure_alert(context):
    task_instance = context["task_instance"]
    exception = context.get("exception")
    send_failure_alert(
        run_id=context["run_id"],
        pipeline_name="health_surveillance_pipeline",
        stage=task_instance.task_id,
        error_message=str(exception),
    )



def extract_load_covid(**context):
    records = extract_covid()
    if not records:
        context["ti"].xcom_push(key="covid_run_id", value=None)
        return
    run_id = records[0]["run_id"]
    load_covid_deaths(records, run_id=run_id)
    context["ti"].xcom_push(key="covid_run_id", value=run_id)


def validate_covid_task(**context):
    run_id = context["ti"].xcom_pull(key="covid_run_id", task_ids="extract_load_covid")
    if run_id is None:
        return  
    result = validate_covid(run_id)
    if not result["success"]:
        raise ValueError(f"COVID validation failed: {result['failed_expectations']} expectation(s) failed")


def anomaly_detect_covid_task(**context):
    run_id = context["ti"].xcom_pull(key="covid_run_id", task_ids="extract_load_covid")
    if run_id is None:
        return
    run_anomaly_detection(run_id)


# ---- Flu pipeline tasks ----

def extract_load_flu(**context):
    records = extract_flu()
    if not records:
        context["ti"].xcom_push(key="flu_run_id", value=None)
        return
    run_id = records[0]["run_id"]
    load_flu_surveillance(records, run_id=run_id)
    context["ti"].xcom_push(key="flu_run_id", value=run_id)


def validate_flu_task(**context):
    run_id = context["ti"].xcom_pull(key="flu_run_id", task_ids="extract_load_flu")
    if run_id is None:
        return
    result = validate_flu(run_id)
    if not result["success"]:
        raise ValueError(f"Flu validation failed: {result['failed_expectations']} expectation(s) failed")


# ---- DAG definition ----

with DAG(
    dag_id="health_surveillance_pipeline",
    default_args=default_args,
    description="Weekly extract-validate-load-observe pipeline for CDC health surveillance data",
    schedule="0 6 * * 6",  # every Saturday at 6am -- CDC publishes Thursdays/Fridays
    start_date=datetime(2026, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert,
    tags=["health", "observability", "covid", "flu"],
) as dag:

    extract_load_covid_task = PythonOperator(
        task_id="extract_load_covid",
        python_callable=extract_load_covid,
    )

    validate_covid_op = PythonOperator(
        task_id="validate_covid",
        python_callable=validate_covid_task,
    )

    anomaly_detect_covid_op = PythonOperator(
        task_id="anomaly_detect_covid",
        python_callable=anomaly_detect_covid_task,
    )

    extract_load_flu_task = PythonOperator(
        task_id="extract_load_flu",
        python_callable=extract_load_flu,
    )

    validate_flu_op = PythonOperator(
        task_id="validate_flu",
        python_callable=validate_flu_task,
    )

    # Two independent branches -- COVID and flu don't depend on each
    # other, so they can run in parallel rather than one waiting on
    # the other unnecessarily.
    extract_load_covid_task >> validate_covid_op >> anomaly_detect_covid_op
    extract_load_flu_task >> validate_flu_op