from storage import minio_client


class FakeS3Client:
    def __init__(self):
        self.copied = None
        self.deleted = None

    def copy_object(self, **kwargs):
        self.copied = kwargs

    def delete_object(self, **kwargs):
        self.deleted = kwargs


def test_move_object_uses_copy_then_delete(monkeypatch):
    fake_client = FakeS3Client()
    monkeypatch.setattr(minio_client, "get_client", lambda: fake_client)

    minio_client.move_object("raw", "landing/a.parquet", "raw", "processing/a.parquet")

    assert fake_client.copied == {
        "Bucket": "raw",
        "Key": "processing/a.parquet",
        "CopySource": {"Bucket": "raw", "Key": "landing/a.parquet"},
    }
    assert fake_client.deleted == {"Bucket": "raw", "Key": "landing/a.parquet"}

