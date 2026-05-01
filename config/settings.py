"""Runtime settings loaded from environment variables.

The defaults are meant for local Docker Compose development. Override them in
production through environment variables instead of editing code.
"""

from __future__ import annotations

import os
from pathlib import Path


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_INTERNAL_ENDPOINT = os.getenv("MINIO_INTERNAL_ENDPOINT", MINIO_ENDPOINT)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_USE_SSL = _get_bool("MINIO_USE_SSL", False)

RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
ICEBERG_BUCKET = os.getenv("ICEBERG_BUCKET", "iceberg")
DLQ_BUCKET = os.getenv("DLQ_BUCKET", "dlq")

PENDING_PREFIX = os.getenv("PENDING_PREFIX", os.getenv("LANDING_PREFIX", "pending/"))
PROCESSING_PREFIX = os.getenv("PROCESSING_PREFIX", "processing/")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX", os.getenv("ARCHIVE_PREFIX", "processed/"))
FAILED_PREFIX = os.getenv("FAILED_PREFIX", "failed/")
DLQ_INVALID_JSON_PREFIX = os.getenv("DLQ_INVALID_JSON_PREFIX", "invalid_json/")
DLQ_SCHEMA_ERRORS_PREFIX = os.getenv("DLQ_SCHEMA_ERRORS_PREFIX", "schema_errors/")
DLQ_SILVER_REJECTS_PREFIX = os.getenv("DLQ_SILVER_REJECTS_PREFIX", "silver_rejects/")

WEBSOCKET_URL = os.getenv("WEBSOCKET_URL", "ws://localhost:8765")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))
LOCAL_BATCH_DIR = Path(os.getenv("LOCAL_BATCH_DIR", "data/tmp"))

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "unity")
ICEBERG_CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "rest")
ICEBERG_WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    f"s3a://{ICEBERG_BUCKET}/warehouse",
)
UC_SERVER_URL = os.getenv("UC_SERVER_URL", "http://unity-catalog:8080")
UC_ICEBERG_REST_URI = os.getenv(
    "UC_ICEBERG_REST_URI",
    f"{UC_SERVER_URL.rstrip('/')}/api/2.1/unity-catalog/iceberg",
)
UC_TOKEN = os.getenv("UC_TOKEN", "not_used")
UC_REST_WAREHOUSE = os.getenv("UC_REST_WAREHOUSE", "unity")
UC_CATALOG = os.getenv("UC_CATALOG", ICEBERG_CATALOG)
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold")
SPARK_REST_FALLBACK_TO_HADOOP = _get_bool("SPARK_REST_FALLBACK_TO_HADOOP", True)
