#!/usr/bin/env bash
set -euo pipefail

UC_BASE_URL="${UC_BASE_URL:-http://unity-catalog:8080}"
UC_API="${UC_BASE_URL}/api/2.1/unity-catalog"
UC_CATALOG_NAME="${UC_CATALOG_NAME:-unity}"

log() {
  echo "[uc-bootstrap] $*"
}

api_get() {
  local url="$1"
  wget -qO- "$url"
}

api_post() {
  local url="$1"
  local payload="$2"
  wget -qO- \
    --header="Content-Type: application/json" \
    --post-data="$payload" \
    "$url" >/dev/null
}

wait_for_api() {
  local retries=60
  local wait_seconds=2
  log "Waiting for Unity Catalog API: ${UC_API}/catalogs"
  for _ in $(seq 1 "$retries"); do
    if api_get "${UC_API}/catalogs" >/dev/null 2>&1; then
      log "Unity Catalog API is ready"
      return 0
    fi
    sleep "$wait_seconds"
  done
  log "Unity Catalog API did not become ready in time"
  exit 1
}

catalog_exists() {
  api_get "${UC_API}/catalogs" | grep -q "\"name\":\"${UC_CATALOG_NAME}\""
}

schema_exists() {
  local schema_name="$1"
  api_get "${UC_API}/schemas?catalog_name=${UC_CATALOG_NAME}" | grep -q "\"name\":\"${schema_name}\""
}

ensure_catalog() {
  if catalog_exists; then
    log "Catalog exists: ${UC_CATALOG_NAME}"
    return 0
  fi

  log "Creating catalog: ${UC_CATALOG_NAME}"
  api_post "${UC_API}/catalogs" "{\"name\":\"${UC_CATALOG_NAME}\",\"comment\":\"Local MinIO lakehouse catalog\"}"
  catalog_exists
  log "Catalog created: ${UC_CATALOG_NAME}"
}

ensure_schema() {
  local schema_name="$1"
  local schema_comment="$2"

  if schema_exists "${schema_name}"; then
    log "Schema exists: ${UC_CATALOG_NAME}.${schema_name}"
    return 0
  fi

  log "Creating schema: ${UC_CATALOG_NAME}.${schema_name}"
  api_post "${UC_API}/schemas" "{\"name\":\"${schema_name}\",\"catalog_name\":\"${UC_CATALOG_NAME}\",\"comment\":\"${schema_comment}\"}"
  schema_exists "${schema_name}"
  log "Schema created: ${UC_CATALOG_NAME}.${schema_name}"
}

main() {
  wait_for_api
  ensure_catalog
  ensure_schema "bronze" "Validated raw events"
  ensure_schema "silver" "Canonical data model"
  ensure_schema "gold" "Business aggregates"
  log "Bootstrap complete"
}

main "$@"
