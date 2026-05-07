#!/usr/bin/env bash
# bootstrap_uc_rest.sh
# Creates the Unity Catalog catalog and bronze/silver/gold schemas via REST API.
# Designed to be idempotent and safe to re-run.

set -uo pipefail          # NOTE: -e intentionally removed — see comments below

UC_BASE_URL="${UC_BASE_URL:-http://unity-catalog:8080}"
UC_API="${UC_BASE_URL}/api/2.1/unity-catalog"
UC_CATALOG_NAME="${UC_CATALOG_NAME:-unity}"

log() {
  echo "[uc-bootstrap] $*"
}

# ---------------------------------------------------------------------------
# api_get: returns response body; exits 0 even on HTTP errors so callers
#          can inspect the response without the script aborting.
# ---------------------------------------------------------------------------
api_get() {
  local url="$1"
  # --server-response would show HTTP status but we only need the body.
  # 2>/dev/null silences wget's own progress lines.
  wget -qO- "$url" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# api_post: fires a JSON POST and discards the response body.
#           Uses || true so any non-zero exit (e.g. HTTP 409 Conflict on an
#           already-existing resource) does NOT abort the script.
# ---------------------------------------------------------------------------
api_post() {
  local url="$1"
  local payload="$2"
  wget -qO- \
    --header="Content-Type: application/json" \
    --post-data="$payload" \
    "$url" 2>/dev/null >/dev/null || true
}

# ---------------------------------------------------------------------------
# wait_for_api: polls the /catalogs endpoint until UC is up.
# ---------------------------------------------------------------------------
wait_for_api() {
  local retries=60
  local wait_seconds=2
  log "Waiting for Unity Catalog API: ${UC_API}/catalogs"
  for _ in $(seq 1 "$retries"); do
    # api_get already has || true so we need to test the content
    local response
    response="$(api_get "${UC_API}/catalogs")"
    if [ -n "$response" ]; then
      log "Unity Catalog API is ready"
      return 0
    fi
    sleep "$wait_seconds"
  done
  log "Unity Catalog API did not become ready in time"
  exit 1
}

# ---------------------------------------------------------------------------
# catalog_exists / schema_exists: grep for the name in the API response.
#   grep -q returns 0 (found) or 1 (not found).
#   We wrap every call in `if` so the 1 is handled, never seen by set -e.
# ---------------------------------------------------------------------------
catalog_exists() {
  api_get "${UC_API}/catalogs" | grep -q "\"name\":\"${UC_CATALOG_NAME}\""
}

schema_exists() {
  local schema_name="$1"
  api_get "${UC_API}/schemas?catalog_name=${UC_CATALOG_NAME}" \
    | grep -q "\"name\":\"${schema_name}\""
}

# ---------------------------------------------------------------------------
# ensure_catalog: OSS Unity Catalog ships with a built-in "unity" catalog.
#   We always try to create; a 409 (already exists) is fine — api_post
#   swallows it via || true.
# ---------------------------------------------------------------------------
ensure_catalog() {
  if catalog_exists; then
    log "Catalog already exists: ${UC_CATALOG_NAME}"
    return 0
  fi

  log "Creating catalog: ${UC_CATALOG_NAME}"
  api_post "${UC_API}/catalogs" \
    "{\"name\":\"${UC_CATALOG_NAME}\",\"comment\":\"Local MinIO lakehouse catalog\"}"

  # Small sleep so UC can persist before we verify
  sleep 1

  if catalog_exists; then
    log "Catalog created: ${UC_CATALOG_NAME}"
  else
    # Not fatal — catalog may have been created by another path or the
    # grep pattern missed it.  Log and continue.
    log "WARNING: Could not verify catalog '${UC_CATALOG_NAME}' — continuing anyway"
  fi
}

# ---------------------------------------------------------------------------
# ensure_schema: creates a schema if it doesn't already exist.
# ---------------------------------------------------------------------------
ensure_schema() {
  local schema_name="$1"
  local schema_comment="$2"

  if schema_exists "${schema_name}"; then
    log "Schema already exists: ${UC_CATALOG_NAME}.${schema_name}"
    return 0
  fi

  log "Creating schema: ${UC_CATALOG_NAME}.${schema_name}"
  api_post "${UC_API}/schemas" \
    "{\"name\":\"${schema_name}\",\"catalog_name\":\"${UC_CATALOG_NAME}\",\"comment\":\"${schema_comment}\"}"

  sleep 1

  if schema_exists "${schema_name}"; then
    log "Schema created: ${UC_CATALOG_NAME}.${schema_name}"
  else
    log "WARNING: Could not verify schema '${schema_name}' — continuing anyway"
  fi
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
main() {
  wait_for_api
  ensure_catalog
  ensure_schema "bronze" "Validated raw events"
  ensure_schema "silver" "Canonical data model"
  ensure_schema "gold"   "Business aggregates"
  log "Bootstrap complete"
}

main "$@"