# MinIO-Backed Uber-Like Event ETL

This project is a modular Python ETL pipeline for Uber-like websocket events.
MinIO is the source of truth for both data and processing state. There is no
Postgres, MySQL, or external SQL database in the application stack.
Open-source Unity Catalog runs locally as the catalog layer for Iceberg tables.

## What Runs

- Ingestion reads websocket JSON events, batches them to Parquet, uploads them
  to `raw/pending/`, and deletes local Parquet files only after upload succeeds.
- Airflow watches `raw/pending/`, moves one file at a time through
  `raw/processing/`, then to `raw/processed/` or `raw/failed/`.
- Spark validates events into Bronze, normalizes the canonical Silver model,
  and builds Gold aggregates as Iceberg tables stored in MinIO under the
  `iceberg` bucket.
- Open-source Unity Catalog exposes the `unity` catalog with `bronze`, `silver`,
  and `gold` schemas through its local API/UI.
- Invalid JSON goes to `dlq/invalid_json/`; schema failures go to
  `dlq/schema_errors/`.

## Event Schema

Every event should include:

```json
{
  "event_id": "evt-001",
  "event_type": "ride_requested",
  "event_ts": "2026-04-20T10:00:00Z"
}
```

Supported event types:

| Event Type | Required Fields |
| --- | --- |
| `new_vehicle` | `vehicle_id`, `type`, `model`, `license_plate` |
| `new_service` | `service_id`, `name`, `service_type` |
| `new_driver` | `driver_id`, `name`, `license_number`, `rating`, `vehicle_id` |
| `new_rider` | `rider_id`, `name`, `email`, `phone` |
| `ride_requested` | `ride_id`, `rider_id`, `driver_id`, `service_id`, `pickup`, `dropoff` |
| `ride_started` | `ride_id`, `driver_id`, `started_at` |
| `trip_completed` | `ride_id`, `distance_km`, `fare`, `completed_at` |
| `payment_completed` | `payment_id`, `ride_id`, `rider_id`, `amount`, `payment_method` |
| `fare_changed` | `service_id`, `old_fare`, `new_fare` |

`pickup` and `dropoff` are JSON objects:

```json
{
  "latitude": 28.6139,
  "longitude": 77.209,
  "address": "Connaught Place"
}
```

## Folder Structure

```text
config/                  Runtime settings
ingestion/               Websocket consumer and Parquet batcher
storage/                 MinIO boto3 wrapper
processing/              Spark Bronze/Silver/Gold Iceberg jobs
orchestration/           Airflow DAG
docker/                  Spark and Airflow Dockerfiles
tests/                   Unit tests
docker-compose.yml       Local infrastructure
requirements.txt         Python dependencies
```

## Start The Stack

Build and start everything:

```bash
docker compose up --build
```

## Links to UIs

Open:

- MinIO console: <http://localhost:9001>
- Airflow UI: <http://localhost:8081>
- Spark master UI: <http://localhost:8082>
- OSS Unity Catalog API: <http://localhost:8083>
- OSS Unity Catalog UI: <http://localhost:3000>

Default credentials:

- MinIO: `minioadmin` / `minioadmin`
- Airflow: `admin` / `admin`

The MinIO init container creates these buckets:

- `raw`
- `iceberg`
- ``dlq``

The Unity Catalog bootstrap container creates or verifies:

- catalog: `unity`
- schemas: `bronze`, `silver`, `gold`

The `raw` bucket uses prefixes as the object-store state machine:

- `pending/`
- `processing/`
- `processed/`
- `failed/`

## Run Ingestion

Install dependencies locally or run inside a Python container with this project
mounted:

```bash
pip install -r requirements.txt
```

Start the websocket consumer:

```bash
set WEBSOCKET_URL=ws://localhost:8765
python -m ingestion.websocket_consumer
```

On macOS/Linux:

```bash
export WEBSOCKET_URL=ws://localhost:8765
python -m ingestion.websocket_consumer
```

## Run Mock WebSocket Server

For local testing without an external event source, start the mock websocket
server in one terminal:

```powershell
python mock_server.py
```

Then start the consumer in another terminal:

```powershell
$env:WEBSOCKET_URL="ws://localhost:8765"
python -m ingestion.websocket_consumer
```

The mock server sends mostly valid ride lifecycle events and occasional invalid
events so the DLQ path can be tested.

Useful environment variables:

| Variable | Default |
| --- | --- |
| `MINIO_ENDPOINT` | `http://localhost:9000` |
| `MINIO_ACCESS_KEY` | `minioadmin` |
| `MINIO_SECRET_KEY` | `minioadmin` |
| `BATCH_SIZE` | `100` |
| `FLUSH_INTERVAL_SECONDS` | `60` |
| `LOCAL_BATCH_DIR` | `data/tmp` |

Unity Catalog and Iceberg settings used by Docker Compose:

| Variable | Default |
| --- | --- |
| `ICEBERG_CATALOG` | `unity` |
| `ICEBERG_CATALOG_TYPE` | `rest` |
| `UC_CATALOG` | `unity` |
| `UC_SERVER_URL` | `http://unity-catalog:8080` |
| `UC_ICEBERG_REST_URI` | `http://unity-catalog:8080/api/2.1/unity-catalog/iceberg` |
| `UC_TOKEN` | `not_used` |
| `UC_REST_WAREHOUSE` | `unity` |
| `SPARK_REST_FALLBACK_TO_HADOOP` | `true` |
| `ICEBERG_WAREHOUSE` | `s3a://iceberg/warehouse` |
| `BRONZE_SCHEMA` | `bronze` |
| `SILVER_SCHEMA` | `silver` |
| `GOLD_SCHEMA` | `gold` |

For a local emergency fallback to the older Hadoop Iceberg catalog, set
`ICEBERG_CATALOG_TYPE=hadoop` and `ICEBERG_WAREHOUSE=s3a://iceberg/warehouse`.

In REST catalog mode (`ICEBERG_CATALOG_TYPE=rest`), Unity Catalog namespaces
must be pre-created. This stack handles that through the
`unity-catalog-bootstrap` service, which initializes `unity.bronze`,
`unity.silver`, and `unity.gold` via REST API calls.

By default this repo also enables `SPARK_REST_FALLBACK_TO_HADOOP=true` to avoid
OSS Unity Catalog REST `405 Method Not Allowed` errors on Iceberg table create.
When enabled, Spark writes tables through an internal Hadoop Iceberg catalog
(`unity_hadoop`) while still keeping Unity Catalog services/UI available.

The Unity Catalog UI service is built locally in this repo and intentionally
proxies API calls to `http://unity-catalog:8080` inside the Docker network.

## Manual Smoke Test

Create and upload a small Parquet batch through the same batcher used by the
websocket consumer:

```bash
python - <<'PY'
from ingestion.batcher import flush_events_to_minio

events = [
    {
        "event_id": "evt-vehicle-1",
        "event_type": "new_vehicle",
        "event_ts": "2026-04-20T10:00:00Z",
        "vehicle_id": "veh-1",
        "type": "sedan",
        "model": "Toyota Camry",
        "license_plate": "DL01AB1234",
    },
    {
        "event_id": "evt-ride-1",
        "event_type": "ride_requested",
        "event_ts": "2026-04-20T10:01:00Z",
        "ride_id": "ride-1",
        "rider_id": "rider-1",
        "driver_id": "driver-1",
        "service_id": "svc-1",
        "pickup": {"latitude": 28.6139, "longitude": 77.209, "address": "Connaught Place"},
        "dropoff": {"latitude": 28.5355, "longitude": 77.391, "address": "Noida"},
    },
]

print(flush_events_to_minio(events))
PY
```

Then open Airflow, enable the `minio_uber_like_event_etl` DAG, and trigger it.
After success:

- the source file should move from `raw/pending/` to `raw/processed/`;
- valid rows should appear as `unity_hadoop.bronze.*`, `unity_hadoop.silver.*`, and `unity_hadoop.gold.*` Iceberg tables when `SPARK_REST_FALLBACK_TO_HADOOP=true` (default);
- table metadata should be visible in the Unity Catalog UI;
- physical Iceberg data files should remain under `iceberg/warehouse/` in MinIO;
- schema failures should appear under `dlq/schema_errors/`.

## Run The Spark Job Manually

If a file is already in `raw/processing/`, run:

```bash
docker compose exec airflow-webserver spark-submit \
  --master spark://spark-master:7077 \
  /opt/airflow/project/processing/spark_job.py \
  --layer bronze \
  --input-bucket raw \
  --input-key processing/YOUR_FILE.parquet
```

To inspect Unity Catalog from the server container:

```bash
docker compose exec unity-catalog bin/uc schema list --catalog unity
docker compose exec unity-catalog bin/uc table list --catalog unity --schema bronze
```

To verify schemas through the REST API:

```bash
docker compose exec airflow-scheduler python -c "import requests; print(requests.get('http://unity-catalog:8080/api/2.1/unity-catalog/schemas?catalog_name=unity', timeout=10).text)"
```

## Troubleshooting

- Spark Bronze error `RESTException ... Status: 405 Method Not Allowed` during
  namespace creation:
  this indicates a REST namespace mutation incompatibility. Ensure
  `unity-catalog-bootstrap` has created `unity.bronze`, `unity.silver`, and
  `unity.gold`.
- Spark Bronze error `RESTException ... Status: 405 Method Not Allowed` during
  table creation (`Builder.create` / `CreateTableExec`):
  this environment does not support Iceberg table create via UC REST. Keep
  `SPARK_REST_FALLBACK_TO_HADOOP=true` (default) so Spark writes through the
  local Hadoop Iceberg catalog instead.
- Spark Bronze error `Missing namespace unity.bronze in REST catalog mode`:
  schema bootstrap has not completed successfully. Recreate bootstrap and
  confirm it exits with code 0 and logs `Bootstrap complete`:
  `docker compose up -d --force-recreate unity-catalog-bootstrap && docker compose logs unity-catalog-bootstrap --tail=200`.
- Unity Catalog UI proxy error `ENOTFOUND server`:
  the UI is trying to proxy to a non-existent host. Rebuild and restart the UI
  service so it uses the repo's custom proxy target (`unity-catalog`).

## Run Tests

```bash
pytest
```

The Airflow DAG test is skipped automatically when Airflow is not installed in
the local Python environment.
