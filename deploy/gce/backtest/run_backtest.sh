#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/aster}"
ENV_FILE="${ENV_FILE:-$APP_DIR/deploy/gce/aster.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

PY_BIN="${APP_DIR}/.venv/bin/python"
BACKTEST_DIR="${APP_DIR}/backtest"
CONFIG_FILE="${ASTER_BACKTEST_CONFIG_FILE:-$BACKTEST_DIR/config_grid.json}"
CONFIG_MAP_CSV="${ASTER_BACKTEST_CONFIG_MAP_CSV:-$BACKTEST_DIR/results/config_mapping.csv}"
RANKED_OUT="${ASTER_BACKTEST_RANKED_CSV:-$BACKTEST_DIR/results/ranked_metrics.csv}"
INPUTS_CSV="${ASTER_BACKTEST_INPUTS_CSV:-$BACKTEST_DIR/backtest_inputs.csv}"
BUILD_INPUTS="${ASTER_BACKTEST_BUILD_INPUTS:-true}"
BQ_PROJECT="${ASTER_BQ_PROJECT:-}"
BQ_DATASET="${ASTER_BQ_DATASET:-aster}"
BQ_LOCATION="${ASTER_BQ_LOCATION:-}"
QUERY_SYMBOLS="${ASTER_BACKTEST_QUERY_SYMBOLS:-}"
QUERY_START_DATE="${ASTER_BACKTEST_START_DATE:-}"
QUERY_END_DATE="${ASTER_BACKTEST_END_DATE:-}"
QUERY_WINDOWS="${ASTER_BACKTEST_FEATURE_WINDOWS:-10,30,60}"

if [[ ! -x "$PY_BIN" ]]; then
  echo "[BACKTEST] Missing python venv at $PY_BIN"
  exit 1
fi

mkdir -p "$BACKTEST_DIR/results"

BUILD_INPUTS_NORM="$(printf '%s' "$BUILD_INPUTS" | tr '[:upper:]' '[:lower:]')"
if [[ "$BUILD_INPUTS_NORM" == "true" ]]; then
  SYMBOLS_CSV="$QUERY_SYMBOLS"
  if [[ -z "$SYMBOLS_CSV" ]]; then
    SYMBOLS_CSV="$("$PY_BIN" -c 'import json,sys; d=json.load(open(sys.argv[1])); s=d.get("symbols", {}); print(",".join([str(x).strip().upper() for x in s.keys() if str(x).strip()]))' "$CONFIG_FILE")"
  fi
  START_DATE="$QUERY_START_DATE"
  END_DATE="$QUERY_END_DATE"
  # Default weekly backtest window (UTC):
  # - end_date: yesterday (typically Saturday when timer runs Sunday 00:20 UTC)
  # - start_date: 28 days before today (typically Sunday 4 weeks prior)
  if [[ -z "$END_DATE" ]]; then
    END_DATE="$(date -u -d '1 day ago' +%F)"
  fi
  if [[ -z "$START_DATE" ]]; then
    START_DATE="$(date -u -d '28 days ago' +%F)"
  fi
  WINDOWS="$QUERY_WINDOWS"

  echo "[BACKTEST] Building backtest inputs from BigQuery..."
  BUILD_CMD=(
    "$PY_BIN" "$BACKTEST_DIR/build_backtest_inputs.py"
    --dataset "$BQ_DATASET"
    --out_csv "$INPUTS_CSV"
    --windows "$WINDOWS"
  )
  if [[ -n "$BQ_PROJECT" ]]; then
    BUILD_CMD+=(--project "$BQ_PROJECT")
  fi
  if [[ -n "$BQ_LOCATION" ]]; then
    BUILD_CMD+=(--location "$BQ_LOCATION")
  fi
  if [[ -n "$SYMBOLS_CSV" ]]; then
    BUILD_CMD+=(--symbols "$SYMBOLS_CSV")
  fi
  if [[ -n "$START_DATE" ]]; then
    BUILD_CMD+=(--start_date "$START_DATE")
  fi
  if [[ -n "$END_DATE" ]]; then
    BUILD_CMD+=(--end_date "$END_DATE")
  fi
  "${BUILD_CMD[@]}"
else
  echo "[BACKTEST] ASTER_BACKTEST_BUILD_INPUTS=false, skipping input rebuild."
fi

echo "[BACKTEST] Running vectorbt backtest..."
"$PY_BIN" "$BACKTEST_DIR/backtest.py" \
  --config_file "$CONFIG_FILE" \
  --inputs_csv "$INPUTS_CSV" \
  --out_config_map_csv "$CONFIG_MAP_CSV" \
  --out_ranked_csv "$RANKED_OUT"

echo "[BACKTEST] Done. Ranked output: $RANKED_OUT"
