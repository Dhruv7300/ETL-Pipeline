"""Small boto3 wrapper for MinIO object operations."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from config import settings


logger = logging.getLogger(__name__)


def get_client():
    """Create an S3-compatible boto3 client pointed at MinIO."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
        region_name=settings.MINIO_REGION,
        use_ssl=settings.MINIO_USE_SSL,
    )


def ensure_buckets(bucket_names: Iterable[str] | None = None) -> None:
    """Create required buckets if they are missing."""
    from botocore.exceptions import ClientError

    client = get_client()
    buckets = list(bucket_names or [settings.RAW_BUCKET, settings.ICEBERG_BUCKET, settings.DLQ_BUCKET])

    for bucket in buckets:
        try:
            client.head_bucket(Bucket=bucket)
            logger.info("Bucket already exists: %s", bucket)
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in {404, 403}:
                raise
            logger.info("Creating bucket: %s", bucket)
            client.create_bucket(Bucket=bucket)


def upload_file(local_path: str | Path, bucket: str, key: str) -> None:
    """Upload a local file to MinIO."""
    path = Path(local_path)
    if not path.exists():
        raise FileNotFoundError(f"Cannot upload missing file: {path}")

    logger.info("Uploading %s to s3://%s/%s", path, bucket, key)
    get_client().upload_file(str(path), bucket, key)


def object_exists(bucket: str, key: str) -> bool:
    """Return True when the object exists."""
    from botocore.exceptions import ClientError

    try:
        get_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = exc.response.get("Error", {}).get("Code")
        if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def move_object(source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> None:
    """Move an object by copying it and deleting the source."""
    client = get_client()
    logger.info(
        "Moving s3://%s/%s to s3://%s/%s",
        source_bucket,
        source_key,
        dest_bucket,
        dest_key,
    )
    client.copy_object(
        Bucket=dest_bucket,
        Key=dest_key,
        CopySource={"Bucket": source_bucket, "Key": source_key},
    )
    client.delete_object(Bucket=source_bucket, Key=source_key)


def list_keys(bucket: str, prefix: str = "") -> list[str]:
    """List object keys under a prefix."""
    client = get_client()
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            keys.append(item["Key"])

    return keys
