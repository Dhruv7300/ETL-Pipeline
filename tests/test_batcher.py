from pathlib import Path
import uuid

import pytest

from ingestion import batcher

TEST_TMP_DIR = Path("data/tmp")


def test_should_flush_by_size():
    buffer = [{"event_id": str(index)} for index in range(100)]

    assert batcher.should_flush(buffer, last_flush_time=0, now=1, max_events=100, interval_seconds=60)


def test_should_flush_by_time():
    buffer = [{"event_id": "evt-1"}]

    assert batcher.should_flush(buffer, last_flush_time=0, now=61, max_events=100, interval_seconds=60)


def test_normalize_event_coerces_invalid_numeric_column_but_preserves_raw_payload():
    event = {
        "event_id": "evt-1",
        "event_ts": "2026-04-20T10:00:00Z",
        "event_type": "trip_completed",
        "ride_id": "ride-1",
        "distance_km": 18.5,
        "fare": "not_a_number",
        "completed_at": "2026-04-20T10:45:00Z",
    }

    normalized = batcher.normalize_event(event)

    assert normalized["fare"] is None
    assert '"fare": "not_a_number"' in normalized["raw_payload"]


def test_write_events_to_parquet_handles_mixed_invalid_numeric_values():
    events = [
        {
            "event_id": "evt-1",
            "event_ts": "2026-04-20T10:00:00Z",
            "event_type": "trip_completed",
            "ride_id": "ride-1",
            "distance_km": 18.5,
            "fare": 420.0,
            "completed_at": "2026-04-20T10:45:00Z",
        },
        {
            "event_id": "evt-2",
            "event_ts": "2026-04-20T11:00:00Z",
            "event_type": "trip_completed",
            "ride_id": "ride-2",
            "distance_km": "12.3",
            "fare": "not_a_number",
            "completed_at": "2026-04-20T11:30:00Z",
        },
    ]

    local_file = batcher.write_events_to_parquet(events, TEST_TMP_DIR)

    assert local_file.exists()
    local_file.unlink(missing_ok=True)


def test_upload_batch_deletes_local_file_after_success(monkeypatch):
    local_file = TEST_TMP_DIR / f"batch_{uuid.uuid4().hex}.parquet"
    local_file.write_text("fake parquet", encoding="utf-8")
    uploaded = {}

    def fake_upload(path, bucket, key):
        uploaded["path"] = Path(path)
        uploaded["bucket"] = bucket
        uploaded["key"] = key

    monkeypatch.setattr(batcher.minio_client, "upload_file", fake_upload)

    key = batcher.upload_batch_file(local_file, "landing/test.parquet")

    assert key == "landing/test.parquet"
    assert uploaded["path"] == local_file
    assert not local_file.exists()


def test_upload_batch_defaults_to_pending_prefix(monkeypatch):
    local_file = TEST_TMP_DIR / f"batch_{uuid.uuid4().hex}.parquet"
    local_file.write_text("fake parquet", encoding="utf-8")
    uploaded = {}

    def fake_upload(path, bucket, key):
        uploaded["path"] = Path(path)
        uploaded["bucket"] = bucket
        uploaded["key"] = key

    monkeypatch.setattr(batcher.minio_client, "upload_file", fake_upload)

    key = batcher.upload_batch_file(local_file)

    assert key.startswith("pending/")
    assert uploaded["key"] == key


def test_upload_batch_keeps_local_file_after_failure(monkeypatch):
    local_file = TEST_TMP_DIR / f"batch_{uuid.uuid4().hex}.parquet"
    local_file.write_text("fake parquet", encoding="utf-8")

    def fake_upload(_path, _bucket, _key):
        raise RuntimeError("upload failed")

    monkeypatch.setattr(batcher.minio_client, "upload_file", fake_upload)

    with pytest.raises(RuntimeError):
        batcher.upload_batch_file(local_file, "landing/test.parquet")

    assert local_file.exists()
    local_file.unlink(missing_ok=True)
