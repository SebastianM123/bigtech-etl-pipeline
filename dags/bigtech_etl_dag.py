"""DAG that orchestrates the daily Big Tech ETL pipeline:
extract from Yahoo Finance, then transform Bronze -> Silver -> Gold."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from extract.fetch_stock_data import run_extraction
from transform.transform_stock_data import (
    run_silver_transformation,
    run_gold_transformation,
)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="bigtech_etl",
    description="Daily ETL pipeline for US Big Tech stock data",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "stocks", "medallion"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=run_extraction,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver_transformation,
    )

    transform_gold = PythonOperator(
        task_id="transform_gold",
        python_callable=run_gold_transformation,
    )

    extract >> transform_silver >> transform_gold