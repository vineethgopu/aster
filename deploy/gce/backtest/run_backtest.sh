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
EMAIL_ON_COMPLETION="${ASTER_EMAIL_BACKTEST_ON_COMPLETION:-true}"
CHUNK_SIZE="${ASTER_BACKTEST_CHUNK_SIZE:-1000}"

if [[ ! -x "$PY_BIN" ]]; then
  echo "[BACKTEST] Missing python venv at $PY_BIN"
  exit 1
fi

mkdir -p "$BACKTEST_DIR/results"

BUILD_INPUTS_NORM="$(printf '%s' "$BUILD_INPUTS" | tr '[:upper:]' '[:lower:]')"
SYMBOLS_CSV="$QUERY_SYMBOLS"
if [[ -z "$SYMBOLS_CSV" ]]; then
  SYMBOLS_CSV="$("$PY_BIN" -c 'import json,sys; d=json.load(open(sys.argv[1])); s=d.get("symbols", {}); print(",".join([str(x).strip().upper() for x in s.keys() if str(x).strip()]))' "$CONFIG_FILE")"
fi
if [[ -z "$SYMBOLS_CSV" ]]; then
  echo "[BACKTEST] No symbols configured."
  exit 1
fi
IFS=',' read -r -a SYMBOLS_ARR <<< "$SYMBOLS_CSV"

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

RANKED_FILES=()
CONFIG_MAP_FILES=()
INPUT_FILES=()

for RAW_SYM in "${SYMBOLS_ARR[@]}"; do
  SYM="$(printf '%s' "$RAW_SYM" | tr -d '[:space:]' | tr '[:lower:]' '[:upper:]')"
  if [[ -z "$SYM" ]]; then
    continue
  fi

  SYM_INPUTS="$BACKTEST_DIR/results/backtest_inputs_${SYM}.csv"
  SYM_CONFIG_MAP="$BACKTEST_DIR/results/config_mapping_${SYM}.csv"
  SYM_RANKED="$BACKTEST_DIR/results/ranked_metrics_${SYM}.csv"

  if [[ "$BUILD_INPUTS_NORM" == "true" ]]; then
    echo "[BACKTEST] Building backtest inputs for $SYM from BigQuery..."
    BUILD_CMD=(
      "$PY_BIN" "$BACKTEST_DIR/build_backtest_inputs.py"
      --dataset "$BQ_DATASET"
      --out_csv "$SYM_INPUTS"
      --windows "$WINDOWS"
      --symbols "$SYM"
    )
    if [[ -n "$BQ_PROJECT" ]]; then
      BUILD_CMD+=(--project "$BQ_PROJECT")
    fi
    if [[ -n "$BQ_LOCATION" ]]; then
      BUILD_CMD+=(--location "$BQ_LOCATION")
    fi
    if [[ -n "$START_DATE" ]]; then
      BUILD_CMD+=(--start_date "$START_DATE")
    fi
    if [[ -n "$END_DATE" ]]; then
      BUILD_CMD+=(--end_date "$END_DATE")
    fi
    "${BUILD_CMD[@]}"
    INPUT_FILES+=("$SYM_INPUTS")
  else
    echo "[BACKTEST] ASTER_BACKTEST_BUILD_INPUTS=false, using shared inputs CSV for $SYM."
    SYM_INPUTS="$INPUTS_CSV"
  fi

  echo "[BACKTEST] Running vectorbt backtest for $SYM (chunk_size=$CHUNK_SIZE)..."
  "$PY_BIN" "$BACKTEST_DIR/backtest.py" \
    --config_file "$CONFIG_FILE" \
    --inputs_csv "$SYM_INPUTS" \
    --out_config_map_csv "$SYM_CONFIG_MAP" \
    --out_ranked_csv "$SYM_RANKED" \
    --symbols "$SYM" \
    --chunk_size "$CHUNK_SIZE"

  CONFIG_MAP_FILES+=("$SYM_CONFIG_MAP")
  RANKED_FILES+=("$SYM_RANKED")
done

if [[ "${#RANKED_FILES[@]}" -eq 0 ]]; then
  echo "[BACKTEST] No ranked outputs produced."
  exit 1
fi

echo "[BACKTEST] Combining per-symbol ranked outputs..."
"$PY_BIN" - "$RANKED_OUT" "${RANKED_FILES[@]}" <<'PY'
import sys
from pathlib import Path
import pandas as pd
out = Path(sys.argv[1])
parts = [pd.read_csv(Path(p)) for p in sys.argv[2:] if Path(p).exists()]
if not parts:
    raise SystemExit("no ranked parts")
df = pd.concat(parts, ignore_index=True)
if "symbol" in df.columns and "total_pnl" in df.columns:
    df = df.sort_values(["symbol", "total_pnl"], ascending=[True, False])
out.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(out, index=False)
print(f"[BACKTEST] wrote combined ranked metrics: {out}")
PY

echo "[BACKTEST] Combining per-symbol config maps..."
"$PY_BIN" - "$CONFIG_MAP_CSV" "${CONFIG_MAP_FILES[@]}" <<'PY'
import sys
from pathlib import Path
import pandas as pd
out = Path(sys.argv[1])
parts = [pd.read_csv(Path(p)) for p in sys.argv[2:] if Path(p).exists()]
if not parts:
    raise SystemExit("no config parts")
df = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["config_id"])
if "symbol" in df.columns and "config_id" in df.columns:
    df = df.sort_values(["symbol", "config_id"])
out.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(out, index=False)
print(f"[BACKTEST] wrote combined config map: {out}")
PY

if [[ "$BUILD_INPUTS_NORM" == "true" && "${#INPUT_FILES[@]}" -gt 0 ]]; then
  echo "[BACKTEST] Combining per-symbol input files..."
  "$PY_BIN" - "$INPUTS_CSV" "${INPUT_FILES[@]}" <<'PY'
import sys
from pathlib import Path
import pandas as pd
out = Path(sys.argv[1])
parts = [pd.read_csv(Path(p)) for p in sys.argv[2:] if Path(p).exists()]
if not parts:
    raise SystemExit("no input parts")
df = pd.concat(parts, ignore_index=True)
if "symbol" in df.columns and "timestamp" in df.columns:
    df = df.sort_values(["symbol", "timestamp"])
out.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(out, index=False)
print(f"[BACKTEST] wrote combined inputs: {out}")
PY
fi

echo "[BACKTEST] Done. Ranked output: $RANKED_OUT"

EMAIL_ON_COMPLETION_NORM="$(printf '%s' "$EMAIL_ON_COMPLETION" | tr '[:upper:]' '[:lower:]')"
if [[ "$EMAIL_ON_COMPLETION_NORM" == "true" ]]; then
  echo "[BACKTEST] Sending backtest completion email..."
  if ! "$PY_BIN" "$APP_DIR/deploy/gce/email_daily_report.py" --mode backtest; then
    echo "[BACKTEST] WARNING: backtest email report failed; backtest outputs are still complete."
  fi
else
  echo "[BACKTEST] ASTER_EMAIL_BACKTEST_ON_COMPLETION=false, skipping completion email."
fi
