"""Batch websocket events into Parquet files and upload them to MinIO."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import settings
from storage import minio_client


logger = logging.getLogger(__name__)

NUMERIC_FIELDS = {
    "rating",
    "distance_km",
    "fare",
    "amount",
    "old_fare",
    "new_fare",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def should_flush(
    buffer: list[dict[str, Any]],
    last_flush_time: float,
    now: float | None = None,
    max_events: int = settings.BATCH_SIZE,
    interval_seconds: int = settings.FLUSH_INTERVAL_SECONDS,
) -> bool:
    """Return True when the in-memory buffer should be flushed."""
    if not buffer:
        return False

    current_time = now if now is not None else time.monotonic()
    return len(buffer) >= max_events or (current_time - last_flush_time) >= interval_seconds


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    """Flatten one event into Parquet-friendly scalar columns."""
    normalized: dict[str, Any] = {}
    enriched_event = dict(event)

    enriched_event.setdefault("event_id", str(uuid.uuid4()))
    enriched_event.setdefault("event_ts", utc_now_iso())

    for key, value in enriched_event.items():
        if isinstance(value, (dict, list)):
            normalized[key] = json.dumps(value, sort_keys=True)
        elif key in NUMERIC_FIELDS:
            normalized[key] = parse_number(value)
        else:
            normalized[key] = value

    normalized["raw_payload"] = json.dumps(enriched_event, sort_keys=True, default=str)
    return normalized


def build_pending_key(local_file: Path) -> str:
    return f"{settings.PENDING_PREFIX}{local_file.name}"


def write_events_to_parquet(events: list[dict[str, Any]], local_dir: str | Path = settings.LOCAL_BATCH_DIR) -> Path:
    """Write events to a local Parquet file and return the file path."""
    if not events:
        raise ValueError("Cannot write an empty event batch")

    import pandas as pd

    output_dir = Path(local_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    local_file = output_dir / f"events_{batch_id}_{uuid.uuid4().hex}.parquet"

    rows = [normalize_event(event) for event in events]
    pd.DataFrame(rows).to_parquet(local_file, engine="pyarrow", index=False)
    logger.info("Wrote %s events to %s", len(events), local_file)
    return local_file


def upload_batch_file(local_file: str | Path, object_key: str | None = None) -> str:
    """Upload a Parquet file and delete it only after a successful upload."""
    path = Path(local_file)
    key = object_key or build_pending_key(path)

    try:
        minio_client.upload_file(path, settings.RAW_BUCKET, key)
    except Exception:
        logger.exception("Upload failed; keeping local file for retry: %s", path)
        raise

    path.unlink(missing_ok=True)
    logger.info("Deleted local batch after successful upload: %s", path)
    return key


def flush_events_to_minio(events: list[dict[str, Any]], local_dir: str | Path = settings.LOCAL_BATCH_DIR) -> str | None:
    """Write a batch to Parquet, upload it to pending, and clean up locally."""
    if not events:
        return None

    local_file = write_events_to_parquet(events, local_dir)
    return upload_batch_file(local_file)


def upload_invalid_json(raw_message: str, error: str, local_dir: str | Path = settings.LOCAL_BATCH_DIR) -> str:
    """Write invalid JSON details to DLQ and clean up the local file after upload."""
    output_dir = Path(local_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    failed_at = utc_now_iso()
    object_name = f"{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{uuid.uuid4().hex}.json"
    local_file = output_dir / f"invalid_json_{uuid.uuid4().hex}.json"
    local_file.write_text(
        json.dumps(
            {
                "raw_message": raw_message,
                "error": error,
                "failed_at": failed_at,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    key = f"{settings.DLQ_INVALID_JSON_PREFIX}{object_name}"
    try:
        minio_client.upload_file(local_file, settings.DLQ_BUCKET, key)
    except Exception:
        logger.exception("Failed to upload invalid JSON record; keeping %s", local_file)
        raise

    local_file.unlink(missing_ok=True)
    logger.info("Uploaded invalid JSON record to s3://%s/%s", settings.DLQ_BUCKET, key)
    return key
