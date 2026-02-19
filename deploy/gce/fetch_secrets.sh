#!/usr/bin/env bash
set -euo pipefail

# Secret names and destination are configurable via env so the same script
# can run across environments (dev/staging/prod) without code changes.
ASTER_SECRET_DIR="${ASTER_SECRET_DIR:-/opt/aster/.secrets}"
ASTER_API_KEY_SECRET_NAME="${ASTER_API_KEY_SECRET_NAME:-aster-api-key}"
ASTER_API_SECRET_SECRET_NAME="${ASTER_API_SECRET_SECRET_NAME:-aster-api-secret}"

# Ensure secret directory exists with restrictive permissions.
# Files written here are plaintext and must not be world-readable.
mkdir -p "$ASTER_SECRET_DIR"
chmod 700 "$ASTER_SECRET_DIR"

# Runtime dependencies:
# - curl for metadata + Secret Manager API requests
# - jq for parsing JSON payloads
if ! command -v curl >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
  echo "Missing required tools: curl and jq."
  exit 1
fi

# Use GCE metadata server to discover:
# - active project ID
# - short-lived OAuth access token for attached VM service account
# This avoids embedding credentials in scripts or env files.
METADATA_HEADER="Metadata-Flavor: Google"
PROJECT_ID="$(curl -fsS -H "$METADATA_HEADER" \
  "http://metadata.google.internal/computeMetadata/v1/project/project-id")"
ACCESS_TOKEN="$(curl -fsS -H "$METADATA_HEADER" \
  "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
  | jq -r '.access_token')"

# Fail closed if metadata/token fetch fails.
if [[ -z "$PROJECT_ID" || -z "$ACCESS_TOKEN" || "$ACCESS_TOKEN" == "null" ]]; then
  echo "Failed to resolve project ID or service account token from metadata server."
  exit 1
fi

# Fetch latest secret payload from Secret Manager REST API and decode it.
# Secret Manager returns base64url content in payload.data.
fetch_secret() {
  local secret_name="$1"
  local out_path="$2"
  local url="https://secretmanager.googleapis.com/v1/projects/${PROJECT_ID}/secrets/${secret_name}/versions/latest:access"
  local json
  json="$(curl -fsS -H "Authorization: Bearer ${ACCESS_TOKEN}" "$url")"
  local data
  data="$(printf '%s' "$json" | jq -r '.payload.data')"
  if [[ -z "$data" || "$data" == "null" ]]; then
    echo "Secret ${secret_name} returned empty payload."
    exit 1
  fi
  # Convert base64url to standard base64 before decoding.
  printf '%s' "$data" | tr '_-' '/+' | base64 --decode > "$out_path"
}

# Write both API credential files used by core/main.py secret loader.
fetch_secret "$ASTER_API_KEY_SECRET_NAME" "$ASTER_SECRET_DIR/api_key"
fetch_secret "$ASTER_API_SECRET_SECRET_NAME" "$ASTER_SECRET_DIR/api_secret"

# Only service user should be able to read generated secret files.
chmod 600 "$ASTER_SECRET_DIR/api_key" "$ASTER_SECRET_DIR/api_secret"

echo "Secrets fetched to $ASTER_SECRET_DIR"
