#!/usr/bin/env bash
set -euo pipefail

# Repo + env defaults.
APP_DIR="${APP_DIR:-/opt/aster}"
ENV_FILE="${ENV_FILE:-$APP_DIR/deploy/gce/aster.env}"

# Load runtime settings if present.
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

LOG_DIR="${ASTER_LOG_DIR:-$APP_DIR/logs}"
RETENTION_DAYS="${ASTER_LOG_RETENTION_DAYS:-7}"
BQ_ENABLE_DAILY_BATCH="${ASTER_BQ_ENABLE_DAILY_BATCH:-true}"
BQ_PROJECT="${ASTER_BQ_PROJECT:-}"
BQ_DATASET="${ASTER_BQ_DATASET:-aster}"
BQ_LOCATION="${ASTER_BQ_LOCATION:-}"
TARGET_DATE_UTC="$(date -u +%Y%m%d)"

echo "[DAILY_BATCH] Stopping aster.service before upload..."
if command -v systemctl >/dev/null 2>&1; then
  systemctl stop aster.service || true
else
  echo "[DAILY_BATCH] systemctl not found; skipping service stop in this environment."
fi

if [[ ! -x "$APP_DIR/.venv/bin/python" ]]; then
  echo "[DAILY_BATCH] Missing python venv at $APP_DIR/.venv"
  exit 1
fi

BQ_ENABLE_NORM="$(printf '%s' "$BQ_ENABLE_DAILY_BATCH" | tr '[:upper:]' '[:lower:]')"
if [[ "$BQ_ENABLE_NORM" == "true" ]]; then
  echo "[DAILY_BATCH] Uploading logs for UTC date ${TARGET_DATE_UTC} to BigQuery..."
  BQ_CMD=(
    "$APP_DIR/.venv/bin/python"
    "$APP_DIR/deploy/gce/bq_load_logs.py"
    --log_dir "$LOG_DIR"
    --dataset "$BQ_DATASET"
    --date "$TARGET_DATE_UTC"
  )
  if [[ -n "$BQ_PROJECT" ]]; then
    BQ_CMD+=(--project "$BQ_PROJECT")
  fi
  if [[ -n "$BQ_LOCATION" ]]; then
    BQ_CMD+=(--location "$BQ_LOCATION")
  fi
  "${BQ_CMD[@]}"
else
  echo "[DAILY_BATCH] ASTER_BQ_ENABLE_DAILY_BATCH=false, skipping BigQuery load."
fi

echo "[DAILY_BATCH] Cleaning logs older than ${RETENTION_DAYS} days from ${LOG_DIR}..."
"$APP_DIR/.venv/bin/python" "$APP_DIR/deploy/gce/cleanup_old_logs.py" \
  --log_dir "$LOG_DIR" \
  --retention_days "$RETENTION_DAYS"

echo "[DAILY_BATCH] Completed."
