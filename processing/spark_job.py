"""PySpark medallion ETL job for validated events, canonical tables, and marts."""

from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Mapping

from config import settings
from processing.uc_registrar import register_table_in_uc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)
HADOOP_FALLBACK_CATALOG = f"{settings.ICEBERG_CATALOG}_hadoop"


COMMON_FIELDS = ("event_id", "event_type", "event_ts")

REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "new_vehicle": ("vehicle_id", "type", "model", "license_plate"),
    "new_service": ("service_id", "name", "service_type"),
    "new_driver": ("driver_id", "name", "license_number", "rating", "vehicle_id"),
    "new_rider": ("rider_id", "name", "email", "phone"),
    "ride_requested": ("ride_id", "rider_id", "driver_id", "service_id", "pickup", "dropoff"),
    "ride_started": ("ride_id", "driver_id", "started_at"),
    "trip_completed": ("ride_id", "distance_km", "fare", "completed_at"),
    "payment_completed": ("payment_id", "ride_id", "rider_id", "amount", "payment_method"),
    "fare_changed": ("service_id", "old_fare", "new_fare"),
}

TABLE_BY_EVENT_TYPE = {
    "new_vehicle": "vehicles",
    "new_service": "services",
    "new_driver": "drivers",
    "new_rider": "riders",
    "ride_requested": "ride_events",
    "ride_started": "ride_events",
    "trip_completed": "ride_events",
    "payment_completed": "payments",
    "fare_changed": "fare_changes",
}

RIDE_STATUS_BY_EVENT_TYPE = {
    "ride_requested": "requested",
    "ride_started": "started",
    "trip_completed": "completed",
}

NUMERIC_FIELDS = {
    "rating",
    "distance_km",
    "fare",
    "amount",
    "old_fare",
    "new_fare",
}

NON_NEGATIVE_NUMERIC_FIELDS = {
    "distance_km",
    "fare",
    "amount",
    "old_fare",
    "new_fare",
}

TIMESTAMP_FIELDS = {"event_ts", "started_at", "completed_at"}
LOCATION_FIELDS = {"pickup", "dropoff"}


def parse_datetime(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def parse_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_location(value: Any) -> dict[str, Any] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None

    if not isinstance(value, Mapping):
        return None

    latitude = parse_number(value.get("latitude"))
    longitude = parse_number(value.get("longitude"))
    if latitude is None or longitude is None:
        return None
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        return None

    return dict(value)


def validate_event_dict(event: Mapping[str, Any]) -> tuple[bool, str]:
    """Validate one event and return (is_valid, error_reason)."""
    if not isinstance(event, Mapping):
        return False, "event must be a JSON object"

    for field in COMMON_FIELDS:
        if is_missing(event.get(field)):
            return False, f"missing required common field: {field}"

    event_type = str(event.get("event_type"))
    if event_type not in REQUIRED_FIELDS:
        return False, f"unsupported event_type: {event_type}"

    if not parse_datetime(event.get("event_ts")):
        return False, "event_ts must be an ISO timestamp"

    for field in REQUIRED_FIELDS[event_type]:
        if is_missing(event.get(field)):
            return False, f"missing required field for {event_type}: {field}"

    for field in REQUIRED_FIELDS[event_type]:
        value = event.get(field)

        if field in NUMERIC_FIELDS:
            number = parse_number(value)
            if number is None:
                return False, f"{field} must be numeric"
            if field in NON_NEGATIVE_NUMERIC_FIELDS and number < 0:
                return False, f"{field} must be non-negative"

        if field == "rating":
            rating = parse_number(value)
            if rating is None or not (0 <= rating <= 5):
                return False, "rating must be between 0 and 5"

        if field in TIMESTAMP_FIELDS and not parse_datetime(value):
            return False, f"{field} must be an ISO timestamp"

        if field in LOCATION_FIELDS and parse_location(value) is None:
            return False, f"{field} must contain latitude and longitude"

    return True, ""


def validate_event_json(payload_json: str | None) -> dict[str, str | bool | None]:
    """Validate a raw JSON payload string. This shape is friendly to Spark UDFs."""
    if not payload_json:
        return {
            "is_valid": False,
            "error_reason": "raw_payload is empty",
            "target_table": None,
        }

    try:
        event = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        return {
            "is_valid": False,
            "error_reason": f"raw_payload is invalid JSON: {exc}",
            "target_table": None,
        }

    is_valid, error_reason = validate_event_dict(event)
    event_type = event.get("event_type") if isinstance(event, Mapping) else None
    return {
        "is_valid": is_valid,
        "error_reason": error_reason,
        "target_table": TABLE_BY_EVENT_TYPE.get(str(event_type)) if is_valid else None,
    }


def create_spark_session(app_name: str = "uber-like-medallion-etl"):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{settings.ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", settings.ICEBERG_CATALOG)
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_INTERNAL_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.MINIO_USE_SSL).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    if settings.ICEBERG_CATALOG_TYPE == "rest":
        builder = (
            builder.config(
                f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.catalog-impl",
                "org.apache.iceberg.rest.RESTCatalog",
            )
            .config(f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.uri", settings.UC_ICEBERG_REST_URI)
            .config(f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.warehouse", settings.UC_REST_WAREHOUSE)
            .config(f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.token", settings.UC_TOKEN)
            .config(f"spark.sql.catalog.{HADOOP_FALLBACK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{HADOOP_FALLBACK_CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{HADOOP_FALLBACK_CATALOG}.warehouse", settings.ICEBERG_WAREHOUSE)
        )
        if settings.SPARK_REST_FALLBACK_TO_HADOOP:
            logger.warning(
                "REST catalog compatibility mode enabled; using %s for Spark table operations",
                HADOOP_FALLBACK_CATALOG,
            )
            builder = builder.config("spark.sql.defaultCatalog", HADOOP_FALLBACK_CATALOG)
    else:
        logger.warning("Using fallback Iceberg Hadoop catalog instead of OSS Unity Catalog REST")
        builder = (
            builder.config(f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.warehouse", settings.ICEBERG_WAREHOUSE)
        )

    return builder.getOrCreate()


def layer_table(layer: str, table_name: str) -> str:
    schema_by_layer = {
        "bronze": settings.BRONZE_SCHEMA,
        "silver": settings.SILVER_SCHEMA,
        "gold": settings.GOLD_SCHEMA,
    }
    return f"{active_catalog_name()}.{schema_by_layer[layer]}.{table_name}"


def active_catalog_name() -> str:
    if settings.ICEBERG_CATALOG_TYPE == "rest" and settings.SPARK_REST_FALLBACK_TO_HADOOP:
        return HADOOP_FALLBACK_CATALOG
    return settings.UC_CATALOG


def table_location(layer: str, table_name: str) -> str:
    schema_by_layer = {
        "bronze": settings.BRONZE_SCHEMA,
        "silver": settings.SILVER_SCHEMA,
        "gold": settings.GOLD_SCHEMA,
    }
    warehouse_root = settings.ICEBERG_WAREHOUSE.rstrip("/")
    return f"{warehouse_root}/{schema_by_layer[layer]}/{table_name}"


def ensure_namespace(spark, layer: str) -> None:
    schema_by_layer = {
        "bronze": settings.BRONZE_SCHEMA,
        "silver": settings.SILVER_SCHEMA,
        "gold": settings.GOLD_SCHEMA,
    }
    target_schema = schema_by_layer[layer]

    catalog_name = active_catalog_name()

    if settings.ICEBERG_CATALOG_TYPE == "rest" and catalog_name == settings.UC_CATALOG:
        logger.info(
            "REST catalog mode detected; skipping CREATE NAMESPACE and validating namespace exists: %s.%s",
            catalog_name,
            target_schema,
        )
        namespaces = spark.sql(f"SHOW NAMESPACES IN {catalog_name}").collect()
        existing_namespaces = set()
        for row in namespaces:
            for value in row:
                if value:
                    existing_namespaces.add(str(value))

        if target_schema not in existing_namespaces:
            raise RuntimeError(
                f"Missing namespace {catalog_name}.{target_schema} in REST catalog mode. "
                "Run the unity-catalog-bootstrap service to create required schemas."
            )
        return

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{target_schema}")


def table_exists(spark, full_table_name: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {full_table_name} LIMIT 1").collect()
        return True
    except Exception:
        return False


def has_rows(df) -> bool:
    return bool(df.take(1))


def source_file_already_loaded(spark, full_table_name: str, source_file: str) -> bool:
    from pyspark.sql import functions as F

    if not table_exists(spark, full_table_name):
        return False

    return spark.table(full_table_name).where(F.col("source_file") == source_file).limit(1).count() > 0


def ensure_raw_payload_column(df):
    from pyspark.sql import functions as F

    if "raw_payload" in df.columns:
        return df

    logger.warning("Input file has no raw_payload column; building it from available columns")
    return df.withColumn("raw_payload", F.to_json(F.struct(*[F.col(column) for column in df.columns])))


def add_missing_columns(df, columns: list[str]):
    from pyspark.sql import functions as F

    for column in columns:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast("string"))
    return df


def validate_dataframe(df):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    validation_schema = T.StructType(
        [
            T.StructField("is_valid", T.BooleanType(), nullable=False),
            T.StructField("error_reason", T.StringType(), nullable=True),
            T.StructField("target_table", T.StringType(), nullable=True),
        ]
    )
    validate_udf = F.udf(validate_event_json, validation_schema)
    return df.withColumn("validation", validate_udf(F.col("raw_payload")))


def write_schema_errors(df) -> None:
    from pyspark.sql import functions as F

    bad_rows = (
        df.where(~F.col("validation.is_valid"))
        .select(
            F.col("event_id").cast("string"),
            F.col("event_type").cast("string"),
            F.col("event_ts").cast("string"),
            F.col("source_file").cast("string"),
            F.col("run_id").cast("string"),
            F.col("validation.error_reason").alias("error_reason"),
            F.col("raw_payload").cast("string"),
            F.current_timestamp().alias("failed_at"),
        )
    )

    if not has_rows(bad_rows):
        return

    dlq_path = f"s3a://{settings.DLQ_BUCKET}/{settings.DLQ_SCHEMA_ERRORS_PREFIX}"
    logger.warning("Writing schema failures to %s", dlq_path)
    bad_rows.write.mode("append").json(dlq_path)


def _column_definitions_for_sql(df) -> str:
    column_defs: list[str] = []
    for field in df.schema.fields:
        column_defs.append(f"`{field.name}` {field.dataType.simpleString()}")
    return ", ".join(column_defs)


def _ensure_iceberg_table_via_sql(
    spark,
    full_table_name: str,
    location: str,
    df,
    partition_columns: list[str] | None = None,
) -> None:
    partition_clause = ""
    if partition_columns:
        partition_expr = ", ".join(f"`{column}`" for column in partition_columns)
        partition_clause = f" PARTITIONED BY ({partition_expr})"
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {full_table_name} "
        f"({_column_definitions_for_sql(df)}) "
        f"USING iceberg{partition_clause} LOCATION '{location}'"
    )
    spark.sql(create_sql)


def append_iceberg_table(spark, layer: str, table_name: str, df, partition_columns: list[str] | None = None) -> None:
    if not has_rows(df):
        logger.info("No rows for %s.%s", layer, table_name)
        return

    ensure_namespace(spark, layer)
    full_table_name = layer_table(layer, table_name)
    if table_exists(spark, full_table_name):
        try:
            df.writeTo(full_table_name).append()
            logger.info("Appended rows to %s", full_table_name)
        except Exception as exc:
            logger.error("Append failed for %s: %s", full_table_name, exc)
            raise
        register_table_in_uc(layer, table_name)
        return

    try:
        if settings.ICEBERG_CATALOG_TYPE == "rest" and active_catalog_name() == settings.UC_CATALOG:
            logger.info("REST catalog mode: creating %s with SQL DDL (non-staging path)", full_table_name)
            _ensure_iceberg_table_via_sql(
                spark,
                full_table_name,
                table_location(layer, table_name),
                df,
                partition_columns,
            )
            df.writeTo(full_table_name).append()
            logger.info("Created and appended rows to %s", full_table_name)
            register_table_in_uc(layer, table_name)
            return

        writer = df.writeTo(full_table_name).using("iceberg").tableProperty("location", table_location(layer, table_name))
        if partition_columns:
            writer = writer.partitionedBy(*partition_columns)
        writer.create()
        logger.info("Created Iceberg table %s", full_table_name)
        register_table_in_uc(layer, table_name)
    except Exception as exc:
        logger.error("Table create/append failed for %s: %s", full_table_name, exc)
        if settings.ICEBERG_CATALOG_TYPE == "rest" and active_catalog_name() == settings.UC_CATALOG:
            logger.error(
                "REST catalog create path failed for %s. "
                "This can indicate endpoint/method incompatibility for table create operations.",
                full_table_name,
            )
        raise


def replace_iceberg_table(spark, layer: str, table_name: str, df, partition_columns: list[str] | None = None) -> None:
    if not has_rows(df):
        logger.info("No rows for %s.%s; skipping replacement", layer, table_name)
        return

    ensure_namespace(spark, layer)
    full_table_name = layer_table(layer, table_name)
    try:
        if settings.ICEBERG_CATALOG_TYPE == "rest" and active_catalog_name() == settings.UC_CATALOG:
            logger.info("REST catalog mode: replacing %s with SQL create + truncate + append", full_table_name)
            _ensure_iceberg_table_via_sql(
                spark,
                full_table_name,
                table_location(layer, table_name),
                df,
                partition_columns,
            )
            spark.sql(f"TRUNCATE TABLE {full_table_name}")
            df.writeTo(full_table_name).append()
            logger.info("Replaced Iceberg table %s", full_table_name)
            register_table_in_uc(layer, table_name)
            return

        writer = df.writeTo(full_table_name).using("iceberg").tableProperty("location", table_location(layer, table_name))
        if partition_columns:
            writer = writer.partitionedBy(*partition_columns)
        writer.createOrReplace()
        logger.info("Replaced Iceberg table %s", full_table_name)
        register_table_in_uc(layer, table_name)
    except Exception as exc:
        logger.error("Table replace failed for %s: %s", full_table_name, exc)
        raise


def process_bronze_file(spark, input_bucket: str, input_key: str, run_id: str) -> None:
    from pyspark.sql import functions as F

    input_path = f"s3a://{input_bucket}/{input_key}"
    full_table_name = layer_table("bronze", "events_valid")
    logger.info("Reading input file for Bronze: %s", input_path)

    ensure_namespace(spark, "bronze")
    if source_file_already_loaded(spark, full_table_name, input_key):
        logger.info("Skipping Bronze load because source file already exists: %s", input_key)
        return

    input_df = spark.read.parquet(input_path)
    input_df = ensure_raw_payload_column(input_df)
    input_df = add_missing_columns(input_df, list(COMMON_FIELDS))

    validated_df = (
        validate_dataframe(input_df)
        .withColumn("source_file", F.lit(input_key))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingested_at", F.current_timestamp())
    )
    validated_df.cache()

    write_schema_errors(validated_df)

    valid_df = (
        validated_df.where(F.col("validation.is_valid"))
        .select(
            F.col("event_id").cast("string").alias("event_id"),
            F.col("event_type").cast("string").alias("event_type"),
            F.to_timestamp(F.col("event_ts")).alias("event_ts"),
            F.col("raw_payload").cast("string").alias("raw_payload"),
            F.col("source_file").cast("string").alias("source_file"),
            F.col("run_id").cast("string").alias("run_id"),
            F.col("ingested_at").alias("ingested_at"),
            F.coalesce(F.to_date(F.col("event_ts")), F.to_date(F.col("ingested_at"))).alias("event_date"),
        )
    )

    append_iceberg_table(spark, "bronze", "events_valid", valid_df, ["event_date"])
    validated_df.unpersist()


def json_col(column: str, path: str):
    from pyspark.sql import functions as F

    return F.get_json_object(F.col(column), path)


def bronze_events(spark):
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    full_table_name = layer_table("bronze", "events_valid")
    if not table_exists(spark, full_table_name):
        raise RuntimeError(f"Bronze table does not exist: {full_table_name}")

    window = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc_nulls_last(), F.col("ingested_at").desc())
    return (
        spark.table(full_table_name)
        .withColumn("row_number", F.row_number().over(window))
        .where(F.col("row_number") == 1)
        .drop("row_number")
        .select(
            F.col("event_id"),
            F.col("event_type"),
            F.col("event_ts"),
            F.col("event_date"),
            F.col("raw_payload"),
            F.col("source_file"),
            F.col("run_id"),
            F.col("ingested_at").alias("bronze_ingested_at"),
            json_col("raw_payload", "$.vehicle_id").alias("vehicle_id"),
            json_col("raw_payload", "$.type").alias("vehicle_type"),
            json_col("raw_payload", "$.model").alias("model"),
            json_col("raw_payload", "$.license_plate").alias("license_plate"),
            json_col("raw_payload", "$.service_id").alias("service_id"),
            json_col("raw_payload", "$.name").alias("name"),
            json_col("raw_payload", "$.service_type").alias("service_type"),
            json_col("raw_payload", "$.driver_id").alias("driver_id"),
            json_col("raw_payload", "$.license_number").alias("license_number"),
            json_col("raw_payload", "$.rating").cast("double").alias("rating"),
            json_col("raw_payload", "$.rider_id").alias("rider_id"),
            json_col("raw_payload", "$.email").alias("email"),
            json_col("raw_payload", "$.phone").alias("phone"),
            json_col("raw_payload", "$.ride_id").alias("ride_id"),
            json_col("raw_payload", "$.pickup.latitude").cast("double").alias("pickup_latitude"),
            json_col("raw_payload", "$.pickup.longitude").cast("double").alias("pickup_longitude"),
            json_col("raw_payload", "$.pickup.address").alias("pickup_address"),
            json_col("raw_payload", "$.dropoff.latitude").cast("double").alias("dropoff_latitude"),
            json_col("raw_payload", "$.dropoff.longitude").cast("double").alias("dropoff_longitude"),
            json_col("raw_payload", "$.dropoff.address").alias("dropoff_address"),
            F.to_timestamp(json_col("raw_payload", "$.started_at")).alias("started_at"),
            F.to_timestamp(json_col("raw_payload", "$.completed_at")).alias("completed_at"),
            json_col("raw_payload", "$.distance_km").cast("double").alias("distance_km"),
            json_col("raw_payload", "$.fare").cast("double").alias("fare"),
            json_col("raw_payload", "$.payment_id").alias("payment_id"),
            json_col("raw_payload", "$.amount").cast("double").alias("amount"),
            json_col("raw_payload", "$.payment_method").alias("payment_method"),
            json_col("raw_payload", "$.old_fare").cast("double").alias("old_fare"),
            json_col("raw_payload", "$.new_fare").cast("double").alias("new_fare"),
        )
    )


def add_lineage_select(df, *columns):
    from pyspark.sql import functions as F

    return df.select(
        *columns,
        F.col("source_file"),
        F.col("run_id"),
        F.col("event_id"),
        F.col("bronze_ingested_at"),
        F.current_timestamp().alias("updated_at"),
    )


def process_silver_canonical_model(spark, _source_file: str | None = None, _run_id: str | None = None) -> None:
    from pyspark.sql import functions as F

    events = bronze_events(spark).cache()

    vehicles = add_lineage_select(
        events.where(F.col("event_type") == "new_vehicle"),
        F.col("vehicle_id"),
        F.col("vehicle_type"),
        F.col("model"),
        F.col("license_plate"),
        F.col("event_ts").alias("effective_at"),
    )
    replace_iceberg_table(spark, "silver", "vehicles", vehicles)

    services = add_lineage_select(
        events.where(F.col("event_type") == "new_service"),
        F.col("service_id"),
        F.col("name"),
        F.col("service_type"),
        F.col("event_ts").alias("effective_at"),
    )
    replace_iceberg_table(spark, "silver", "services", services)

    drivers = add_lineage_select(
        events.where(F.col("event_type") == "new_driver"),
        F.col("driver_id"),
        F.col("name"),
        F.col("license_number"),
        F.col("rating"),
        F.col("vehicle_id"),
        F.col("event_ts").alias("effective_at"),
    )
    replace_iceberg_table(spark, "silver", "drivers", drivers)

    riders = add_lineage_select(
        events.where(F.col("event_type") == "new_rider"),
        F.col("rider_id"),
        F.col("name"),
        F.col("email"),
        F.col("phone"),
        F.col("event_ts").alias("effective_at"),
    )
    replace_iceberg_table(spark, "silver", "riders", riders)

    ride_events = add_lineage_select(
        events.where(F.col("event_type").isin("ride_requested", "ride_started", "trip_completed"))
        .withColumn(
            "ride_status",
            F.when(F.col("event_type") == "ride_requested", F.lit(RIDE_STATUS_BY_EVENT_TYPE["ride_requested"]))
            .when(F.col("event_type") == "ride_started", F.lit(RIDE_STATUS_BY_EVENT_TYPE["ride_started"]))
            .when(F.col("event_type") == "trip_completed", F.lit(RIDE_STATUS_BY_EVENT_TYPE["trip_completed"])),
        ),
        F.col("ride_id"),
        F.col("event_type"),
        F.col("ride_status"),
        F.col("event_ts"),
        F.col("rider_id"),
        F.col("driver_id"),
        F.col("service_id"),
        F.col("pickup_latitude"),
        F.col("pickup_longitude"),
        F.col("pickup_address"),
        F.col("dropoff_latitude"),
        F.col("dropoff_longitude"),
        F.col("dropoff_address"),
        F.col("started_at"),
        F.col("completed_at"),
        F.col("distance_km"),
        F.col("fare"),
        F.col("event_date"),
    )
    replace_iceberg_table(spark, "silver", "ride_events", ride_events, ["event_date"])

    rides = (
        ride_events.groupBy("ride_id")
        .agg(
            F.first("rider_id", ignorenulls=True).alias("rider_id"),
            F.first("driver_id", ignorenulls=True).alias("driver_id"),
            F.first("service_id", ignorenulls=True).alias("service_id"),
            F.min(F.when(F.col("event_type") == "ride_requested", F.col("event_ts"))).alias("requested_at"),
            F.min(F.when(F.col("event_type") == "ride_started", F.coalesce(F.col("started_at"), F.col("event_ts")))).alias(
                "started_at"
            ),
            F.min(F.when(F.col("event_type") == "trip_completed", F.coalesce(F.col("completed_at"), F.col("event_ts")))).alias(
                "completed_at"
            ),
            F.first("pickup_latitude", ignorenulls=True).alias("pickup_latitude"),
            F.first("pickup_longitude", ignorenulls=True).alias("pickup_longitude"),
            F.first("pickup_address", ignorenulls=True).alias("pickup_address"),
            F.first("dropoff_latitude", ignorenulls=True).alias("dropoff_latitude"),
            F.first("dropoff_longitude", ignorenulls=True).alias("dropoff_longitude"),
            F.first("dropoff_address", ignorenulls=True).alias("dropoff_address"),
            F.max("distance_km").alias("distance_km"),
            F.max("fare").alias("fare"),
            F.max("source_file").alias("source_file"),
            F.max("run_id").alias("run_id"),
            F.max("bronze_ingested_at").alias("bronze_ingested_at"),
        )
        .withColumn(
            "ride_status",
            F.when(F.col("completed_at").isNotNull(), F.lit("completed"))
            .when(F.col("started_at").isNotNull(), F.lit("started"))
            .otherwise(F.lit("requested")),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    replace_iceberg_table(spark, "silver", "rides", rides)

    payments = add_lineage_select(
        events.where(F.col("event_type") == "payment_completed"),
        F.col("payment_id"),
        F.col("ride_id"),
        F.col("rider_id"),
        F.col("amount"),
        F.col("payment_method"),
        F.col("event_ts"),
        F.col("event_date"),
    )
    replace_iceberg_table(spark, "silver", "payments", payments, ["event_date"])

    fare_changes = add_lineage_select(
        events.where(F.col("event_type") == "fare_changed"),
        F.col("service_id"),
        F.col("old_fare"),
        F.col("new_fare"),
        F.col("event_ts"),
        F.col("event_date"),
    )
    replace_iceberg_table(spark, "silver", "fare_changes", fare_changes, ["event_date"])

    events.unpersist()


def process_gold_aggregates(spark, _source_file: str | None = None, _run_id: str | None = None) -> None:
    from pyspark.sql import functions as F

    rides_table = layer_table("silver", "rides")
    payments_table = layer_table("silver", "payments")
    if not table_exists(spark, rides_table):
        raise RuntimeError(f"Silver rides table does not exist: {rides_table}")

    rides = spark.table(rides_table).cache()
    completed_rides = rides.where(F.col("completed_at").isNotNull())

    daily_ride_kpis = (
        rides.withColumn("business_date", F.to_date(F.coalesce(F.col("requested_at"), F.col("started_at"), F.col("completed_at"))))
        .groupBy("business_date")
        .agg(
            F.count("*").alias("total_rides_requested"),
            F.sum(F.when(F.col("completed_at").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("total_rides_completed"),
            F.sum(F.coalesce(F.col("fare"), F.lit(0.0))).alias("total_revenue"),
            F.avg("fare").alias("average_fare"),
            F.avg("distance_km").alias("average_distance_km"),
        )
        .withColumn("completion_rate", F.col("total_rides_completed") / F.col("total_rides_requested"))
        .withColumn("updated_at", F.current_timestamp())
    )
    replace_iceberg_table(spark, "gold", "daily_ride_kpis", daily_ride_kpis, ["business_date"])

    driver_performance_daily = (
        completed_rides.withColumn("business_date", F.to_date("completed_at"))
        .groupBy("business_date", "driver_id")
        .agg(
            F.count("*").alias("trips_completed"),
            F.sum(F.coalesce(F.col("fare"), F.lit(0.0))).alias("revenue"),
            F.avg("distance_km").alias("average_distance_km"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    replace_iceberg_table(spark, "gold", "driver_performance_daily", driver_performance_daily, ["business_date"])

    service_revenue_daily = (
        completed_rides.withColumn("business_date", F.to_date("completed_at"))
        .groupBy("business_date", "service_id")
        .agg(
            F.count("*").alias("trips_completed"),
            F.sum(F.coalesce(F.col("fare"), F.lit(0.0))).alias("revenue"),
            F.avg("fare").alias("average_fare"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    replace_iceberg_table(spark, "gold", "service_revenue_daily", service_revenue_daily, ["business_date"])

    if table_exists(spark, payments_table):
        payment_method_daily = (
            spark.table(payments_table)
            .withColumn("business_date", F.to_date("event_ts"))
            .groupBy("business_date", "payment_method")
            .agg(
                F.count("*").alias("payment_count"),
                F.sum(F.coalesce(F.col("amount"), F.lit(0.0))).alias("payment_amount"),
            )
            .withColumn("updated_at", F.current_timestamp())
        )
        replace_iceberg_table(spark, "gold", "payment_method_daily", payment_method_daily, ["business_date"])

    rides.unpersist()


def process_file(spark, input_bucket: str, input_key: str, run_id: str) -> None:
    """Compatibility wrapper for callers that still expect a single-file Bronze load."""
    process_bronze_file(spark, input_bucket, input_key, run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one medallion ETL layer.")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], default="bronze")
    parser.add_argument("--input-bucket", default=settings.RAW_BUCKET)
    parser.add_argument("--input-key")
    parser.add_argument("--source-file")
    parser.add_argument("--run-id")
    args = parser.parse_args()

    run_id = args.run_id or str(uuid.uuid4())
    source_file = args.source_file or args.input_key

    spark = create_spark_session()
    try:
        if args.layer == "bronze":
            if not args.input_key:
                raise ValueError("--input-key is required for the bronze layer")
            process_bronze_file(spark, args.input_bucket, args.input_key, run_id)
        elif args.layer == "silver":
            process_silver_canonical_model(spark, source_file, run_id)
        elif args.layer == "gold":
            process_gold_aggregates(spark, source_file, run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
