"""
processing/uc_registrar.py
--------------------------
Registers Iceberg tables written via the Hadoop catalog into OSS Unity Catalog
using UC's native /api/2.1/unity-catalog/tables REST endpoint.

Why this is needed
------------------
OSS Unity Catalog does NOT implement the full Iceberg REST catalog spec for
table *creation* — it returns 405 Method Not Allowed on POST /v1/tables.
Spark therefore writes physical Iceberg files to MinIO via the fallback Hadoop
catalog (`unity_hadoop`).  Once the data is on disk, this module announces each
table to Unity Catalog through UC's own API so the table appears in the UI.

The registration call is idempotent: a 409 Conflict response (table already
registered) is treated as success so re-runs are safe.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _uc_base_url() -> str:
    return os.environ.get("UC_SERVER_URL", "http://unity-catalog:8083")


def _headers() -> dict[str, str]:
    token = os.environ.get("UC_TOKEN", "not_used")
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _schema_for_layer(layer: str) -> str:
    mapping = {
        "bronze": os.environ.get("BRONZE_SCHEMA", "bronze"),
        "silver": os.environ.get("SILVER_SCHEMA", "silver"),
        "gold":   os.environ.get("GOLD_SCHEMA",   "gold"),
    }
    return mapping[layer]


def _table_storage_location(layer: str, table_name: str) -> str:
    """
    Mirrors the logic in spark_job.py::table_location() so the S3 path
    registered in UC matches exactly where Spark wrote the Iceberg data.

    Result example: s3a://iceberg/warehouse/bronze/events_valid
    """
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3a://iceberg/warehouse").rstrip("/")
    schema = _schema_for_layer(layer)
    return f"{warehouse}/{schema}/{table_name}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def register_table_in_uc(
    layer: str,
    table_name: str,
    catalog: Optional[str] = None,
    storage_location: Optional[str] = None,
) -> bool:
    """
    Register one Iceberg table in Unity Catalog.

    Parameters
    ----------
    layer:            "bronze", "silver", or "gold"
    table_name:       Bare table name, e.g. "events_valid"
    catalog:          UC catalog name — defaults to env UC_CATALOG ("unity")
    storage_location: Full s3a:// path; auto-computed from ICEBERG_WAREHOUSE
                      if omitted.

    Returns True on success or if already registered, False on transient error.
    """
    if catalog is None:
        catalog = os.environ.get("UC_CATALOG", "unity")

    if storage_location is None:
        storage_location = _table_storage_location(layer, table_name)

    schema = _schema_for_layer(layer)
    url = f"{_uc_base_url()}/api/2.1/unity-catalog/tables"

    payload = {
        "name": table_name,
        "catalog_name": catalog,
        "schema_name": schema,
        "table_type": "EXTERNAL",
        # UC's native API uses DELTA for external Iceberg tables in OSS UC.
        # The physical format is still Iceberg on disk — this is just the UC
        # metadata label required for external table registration to succeed.
        "data_source_format": "DELTA",
        "storage_location": storage_location,
        "columns": [],
    }

    try:
        resp = requests.post(url, json=payload, headers=_headers(), timeout=15)

        if resp.status_code in (200, 201):
            logger.info(
                "UC registration OK: %s.%s.%s → %s",
                catalog, schema, table_name, storage_location,
            )
            return True

        if resp.status_code == 409:
            logger.info(
                "UC registration skipped (already exists): %s.%s.%s",
                catalog, schema, table_name,
            )
            return True

        # Non-fatal: log the problem but let the Spark job succeed.
        # Tables can be registered manually later without re-running Spark.
        logger.warning(
            "UC registration returned HTTP %s for %s.%s.%s — %s",
            resp.status_code, catalog, schema, table_name, resp.text,
        )
        return False

    except requests.exceptions.RequestException as exc:
        # Also non-fatal: Unity Catalog might be temporarily unreachable.
        logger.warning(
            "Could not reach Unity Catalog to register %s.%s.%s: %s",
            catalog, schema, table_name, exc,
        )
        return False
