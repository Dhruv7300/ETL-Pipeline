"""Airflow DAG for MinIO-backed ETL state transitions."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

from config import settings
from storage import minio_client


logger = logging.getLogger(__name__)


def create_run_id(**_context) -> str:
    return str(uuid.uuid4())


def pick_pending_key(**_context) -> str:
    keys = sorted(
        key
        for key in minio_client.list_keys(settings.RAW_BUCKET, settings.PENDING_PREFIX)
        if key.endswith(".parquet")
    )
    if not keys:
        raise FileNotFoundError(f"No Parquet files found under {settings.RAW_BUCKET}/{settings.PENDING_PREFIX}")
    logger.info("Picked pending object: %s", keys[0])
    return keys[0]


def move_pending_to_processing(**context) -> str:
    source_key = context["ti"].xcom_pull(task_ids="pick_pending_key")
    file_name = source_key.rsplit("/", 1)[-1]
    processing_key = f"{settings.PROCESSING_PREFIX}{file_name}"

    minio_client.move_object(settings.RAW_BUCKET, source_key, settings.RAW_BUCKET, processing_key)
    logger.info("Moved pending object to processing: %s", processing_key)
    return processing_key


def move_processing_to_final(status: str, **context) -> str | None:
    processing_key = context["ti"].xcom_pull(task_ids="move_pending_to_processing")
    if not processing_key:
        logger.warning("No processing key found in XCom; nothing to move")
        return None

    if not minio_client.object_exists(settings.RAW_BUCKET, processing_key):
        logger.warning("Processing object no longer exists: %s", processing_key)
        return None

    file_name = processing_key.rsplit("/", 1)[-1]
    final_prefix = settings.PROCESSED_PREFIX if status == "processed" else settings.FAILED_PREFIX
    final_key = f"{final_prefix}{file_name}"

    minio_client.move_object(settings.RAW_BUCKET, processing_key, settings.RAW_BUCKET, final_key)
    logger.info("Moved processing object to %s: %s", status, final_key)
    return final_key


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

spark_conf = {
    "spark.driverEnv.PYTHONPATH": "/opt/airflow/project",
    "spark.executorEnv.PYTHONPATH": "/opt/spark/project",
    "spark.executorEnv.MINIO_ENDPOINT": settings.MINIO_INTERNAL_ENDPOINT,
    "spark.executorEnv.MINIO_INTERNAL_ENDPOINT": settings.MINIO_INTERNAL_ENDPOINT,
    "spark.executorEnv.MINIO_ACCESS_KEY": settings.MINIO_ACCESS_KEY,
    "spark.executorEnv.MINIO_SECRET_KEY": settings.MINIO_SECRET_KEY,
    "spark.executorEnv.ICEBERG_CATALOG": settings.ICEBERG_CATALOG,
    "spark.executorEnv.ICEBERG_CATALOG_TYPE": settings.ICEBERG_CATALOG_TYPE,
    "spark.executorEnv.UC_CATALOG": settings.UC_CATALOG,
    "spark.executorEnv.UC_SERVER_URL": settings.UC_SERVER_URL,
    "spark.executorEnv.UC_ICEBERG_REST_URI": settings.UC_ICEBERG_REST_URI,
    "spark.executorEnv.UC_TOKEN": settings.UC_TOKEN,
    "spark.executorEnv.UC_REST_WAREHOUSE": settings.UC_REST_WAREHOUSE,
    "spark.executorEnv.BRONZE_SCHEMA": settings.BRONZE_SCHEMA,
    "spark.executorEnv.SILVER_SCHEMA": settings.SILVER_SCHEMA,
    "spark.executorEnv.GOLD_SCHEMA": settings.GOLD_SCHEMA,
}


with DAG(
    dag_id="minio_uber_like_event_etl",
    description="Move MinIO pending files through Bronze, Silver, and Gold Iceberg tables.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["minio", "spark", "iceberg"],
) as dag:
    wait_for_pending_file = S3KeySensor(
        task_id="wait_for_pending_file",
        bucket_name=settings.RAW_BUCKET,
        bucket_key=f"{settings.PENDING_PREFIX}*.parquet",
        wildcard_match=True,
        aws_conn_id="minio_s3",
        poke_interval=20,
        timeout=60 * 60,
        mode="reschedule",
    )

    make_run_id = PythonOperator(
        task_id="create_run_id",
        python_callable=create_run_id,
    )

    pick_pending_file = PythonOperator(
        task_id="pick_pending_key",
        python_callable=pick_pending_key,
    )

    move_to_processing = PythonOperator(
        task_id="move_pending_to_processing",
        python_callable=move_pending_to_processing,
    )

    submit_bronze_job = SparkSubmitOperator(
        task_id="submit_bronze_job",
        conn_id="spark_default",
        application="/opt/airflow/project/processing/spark_job.py",
        application_args=[
            "--layer",
            "bronze",
            "--input-bucket",
            settings.RAW_BUCKET,
            "--input-key",
            "{{ ti.xcom_pull(task_ids='move_pending_to_processing') }}",
            "--run-id",
            "{{ ti.xcom_pull(task_ids='create_run_id') }}",
        ],
        conf=spark_conf,
        verbose=True,
    )

    submit_silver_job = SparkSubmitOperator(
        task_id="submit_silver_job",
        conn_id="spark_default",
        application="/opt/airflow/project/processing/spark_job.py",
        application_args=[
            "--layer",
            "silver",
            "--source-file",
            "{{ ti.xcom_pull(task_ids='move_pending_to_processing') }}",
            "--run-id",
            "{{ ti.xcom_pull(task_ids='create_run_id') }}",
        ],
        conf=spark_conf,
        verbose=True,
    )

    submit_gold_job = SparkSubmitOperator(
        task_id="submit_gold_job",
        conn_id="spark_default",
        application="/opt/airflow/project/processing/spark_job.py",
        application_args=[
            "--layer",
            "gold",
            "--source-file",
            "{{ ti.xcom_pull(task_ids='move_pending_to_processing') }}",
            "--run-id",
            "{{ ti.xcom_pull(task_ids='create_run_id') }}",
        ],
        conf=spark_conf,
        verbose=True,
    )

    move_to_processed = PythonOperator(
        task_id="move_to_processed",
        python_callable=move_processing_to_final,
        op_kwargs={"status": "processed"},
    )

    move_to_failed = PythonOperator(
        task_id="move_to_failed",
        python_callable=move_processing_to_final,
        op_kwargs={"status": "failed"},
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    wait_for_pending_file >> make_run_id >> pick_pending_file >> move_to_processing
    move_to_processing >> submit_bronze_job >> submit_silver_job >> submit_gold_job >> move_to_processed
    [submit_bronze_job, submit_silver_job, submit_gold_job] >> move_to_failed
