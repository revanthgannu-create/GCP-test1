import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# Change these values as per your project
project_id = "revadata1"
region = "us-central1"
bucket_name = "revadataproc"
cluster_name = "sample"


def spark_sql_job(query_file):
    return {
        "reference": {"project_id": project_id},
        "placement": {"cluster_name": cluster_name},
        "spark_sql_job": {
            "query_file_uri": f"gs://{bucket_name}/scripts/{query_file}"
        },
    }


with models.DAG(
    "Usecase3_Job_WithNoParams",
    start_date=days_ago(1),
    schedule_interval=datetime.timedelta(days=1),
    catchup=False,
) as dag:

    drop = DataprocSubmitJobOperator(
        task_id="drop",
        job=spark_sql_job("drop.sql"),
        region=region,
    )

    customer_lo = DataprocSubmitJobOperator(
        task_id="customer_lo",
        job=spark_sql_job("customer_lo.sql"),
        region=region,
    )

    customer_ny = DataprocSubmitJobOperator(
        task_id="customer_ny",
        job=spark_sql_job("customer_ny.sql"),
        region=region,
    )

    salesman_lo = DataprocSubmitJobOperator(
        task_id="salesman_lo",
        job=spark_sql_job("salesman_lo.sql"),
        region=region,
    )

    salesman_ny = DataprocSubmitJobOperator(
        task_id="salesman_ny",
        job=spark_sql_job("salesman_ny.sql"),
        region=region,
    )

    orders = DataprocSubmitJobOperator(
        task_id="orders",
        job=spark_sql_job("orders.sql"),
        region=region,
    )

    results_summary = DataprocSubmitJobOperator(
        task_id="results_summary",
        job=spark_sql_job("result_summary.sql"),
        region=region,
    )

    drop >> [
        customer_lo,
        customer_ny,
        salesman_lo,
        salesman_ny,
        orders,
    ] >> results_summary