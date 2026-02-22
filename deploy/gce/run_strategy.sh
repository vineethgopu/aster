#!/usr/bin/env bash
set -euo pipefail

# APP_DIR is the VM checkout root.
# ENV_FILE contains non-secret runtime config (symbols, sizing, toggles).
APP_DIR="${APP_DIR:-/opt/aster}"
ENV_FILE="${ENV_FILE:-$APP_DIR/deploy/gce/aster.env}"

# Load runtime parameters if present.
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

# Fetch fresh secret material on each start/restart.
# This allows secret rotation without rebuilding the image.
"$APP_DIR/deploy/gce/fetch_secrets.sh"

# core/main.py expects ORDER_API_KEY and ORDER_SECRET_KEY to be file paths.
export ORDER_API_KEY="${ASTER_SECRET_DIR:-/opt/aster/.secrets}/api_key"
export ORDER_SECRET_KEY="${ASTER_SECRET_DIR:-/opt/aster/.secrets}/api_secret"

# Fail fast if secret fetch did not produce files.
if [[ ! -f "$ORDER_API_KEY" || ! -f "$ORDER_SECRET_KEY" ]]; then
  echo "Missing secret files."
  exit 1
fi

# Activate project-local venv and run from /core so local imports resolve.
# shellcheck disable=SC1091
source "$APP_DIR/.venv/bin/activate"
cd "$APP_DIR/core"

# exec replaces shell with python process so systemd tracks true PID.
exec python main.py \
  --symbols "${ASTER_SYMBOLS:-ETHUSDT}" \
  --poll_time "${ASTER_POLL_TIME:-600}" \
  --log_dir "${ASTER_LOG_DIR:-/opt/aster/logs}" \
  --delete_logs "${ASTER_DELETE_LOGS:-false}" \
  --update_logs "${ASTER_UPDATE_LOGS:-true}" \
  --enable_trading "${ASTER_ENABLE_TRADING:-false}" \
  --order_notional "${ASTER_ORDER_NOTIONAL:-5.0}" \
  --k "${ASTER_K:-1.3}" \
  --T "${ASTER_T:-30}" \
  --n "${ASTER_N:-1.3}" \
  --V "${ASTER_V:-30}" \
  --max_spread "${ASTER_MAX_SPREAD:-0.2}" \
  --max_funding_abs_bps "${ASTER_MAX_FUNDING_ABS_BPS:-1.5}" \
  --taker_fee_bps "${ASTER_TAKER_FEE_BPS:-4.0}" \
  --take_profit_mult "${ASTER_TAKE_PROFIT_MULT:-3.0}" \
  --stop_loss_bps "${ASTER_STOP_LOSS_BPS:--1.0}" \
  --trailing_activation_frac "${ASTER_TRAILING_ACTIVATION_FRAC:-0.5}" \
  --trailing_callback_rate "${ASTER_TRAILING_CALLBACK_RATE:--1.0}" \
  --margin_safety_multiple "${ASTER_MARGIN_SAFETY_MULTIPLE:-1.2}" \
  --reentry_cooldown_min "${ASTER_REENTRY_COOLDOWN_MIN:-10}" \
  --entry_halt_utc "${ASTER_ENTRY_HALT_UTC:-23:00}" \
  --force_exit_utc "${ASTER_FORCE_EXIT_UTC:-23:50}"
