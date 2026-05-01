import importlib

import pytest


def test_airflow_dag_imports_when_airflow_is_available():
    pytest.importorskip("airflow")

    try:
        module = importlib.import_module("orchestration.airflow_dag")
    except ModuleNotFoundError as exc:
        pytest.skip(f"Airflow provider install is incomplete in this environment: {exc}")

    assert module.dag.dag_id == "minio_uber_like_event_etl"
