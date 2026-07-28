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

FETCH_SECRETS_NORM="$(printf '%s' "${ASTER_FETCH_SECRETS:-true}" | tr '[:upper:]' '[:lower:]')"
if [[ "$FETCH_SECRETS_NORM" == "true" ]]; then
  # Fetch fresh secret material on each start/restart.
  # This allows secret rotation without rebuilding the image.
  "$APP_DIR/deploy/gce/fetch_secrets.sh"
else
  echo "[RUN_STRATEGY] ASTER_FETCH_SECRETS=false; skipping Secret Manager fetch."
fi

# Runtime entrypoints expect ORDER_API_KEY and ORDER_SECRET_KEY to be file paths
# when trading is enabled.
export ORDER_API_KEY="${ASTER_SECRET_DIR:-/opt/aster/.secrets}/api_key"
export ORDER_SECRET_KEY="${ASTER_SECRET_DIR:-/opt/aster/.secrets}/api_secret"

ENABLE_TRADING_NORM="$(printf '%s' "${ASTER_ENABLE_TRADING:-false}" | tr '[:upper:]' '[:lower:]')"
if [[ "$ENABLE_TRADING_NORM" == "true" ]]; then
  # Fail fast if secret fetch did not produce files for live trading.
  if [[ ! -f "$ORDER_API_KEY" || ! -f "$ORDER_SECRET_KEY" ]]; then
    echo "Missing secret files."
    exit 1
  fi
fi

# Activate project-local venv and run from /core so local imports resolve.
# shellcheck disable=SC1091
source "$APP_DIR/.venv/bin/activate"
cd "$APP_DIR/core"

STRATEGY_MODE="$(printf '%s' "${ASTER_STRATEGY_MODE:-legacy}" | tr '[:upper:]' '[:lower:]')"

if [[ "$STRATEGY_MODE" == "portfolio" || "$STRATEGY_MODE" == "cross_sectional_momentum" ]]; then
  CMD=(python portfolio_main.py \
    --config_file "${ASTER_PORTFOLIO_CONFIG_FILE:-/opt/aster/core/config_cross_sectional_momentum.json}" \
    --poll_time "${ASTER_POLL_TIME:-600}" \
    --log_dir "${ASTER_LOG_DIR:-/opt/aster/logs}" \
    --delete_logs "${ASTER_DELETE_LOGS:-false}" \
    --update_logs "${ASTER_UPDATE_LOGS:-true}" \
    --enable_trading "${ASTER_ENABLE_TRADING:-false}")

  if [[ -n "${ASTER_TARGET_LEVERAGE:-}" ]]; then
    CMD+=(--target_leverage "${ASTER_TARGET_LEVERAGE}")
  fi
  if [[ -n "${ASTER_MARGIN_TYPE:-}" ]]; then
    CMD+=(--margin_type "${ASTER_MARGIN_TYPE}")
  fi

  echo "[RUN_STRATEGY] strategy_mode=portfolio cmd=${CMD[*]}"
  exec "${CMD[@]}"
fi

# Build legacy command dynamically:
# - order_notional is optional: if omitted, main.py computes default from
#   start-of-day balance * risk_pct * leverage.
CMD=(python main.py \
  --symbols "${ASTER_SYMBOLS:-ETHUSDT}" \
  --poll_time "${ASTER_POLL_TIME:-600}" \
  --log_dir "${ASTER_LOG_DIR:-/opt/aster/logs}" \
  --delete_logs "${ASTER_DELETE_LOGS:-false}" \
  --update_logs "${ASTER_UPDATE_LOGS:-true}" \
  --enable_trading "${ASTER_ENABLE_TRADING:-false}" \
  --target_leverage "${ASTER_TARGET_LEVERAGE:-25}" \
  --risk_pct "${ASTER_RISK_PCT:-1.0}" \
  --trade_alert_email "${ASTER_EMAIL_TRADE_ALERT_ENABLE:-true}" \
  --k "${ASTER_K:-1.3}" \
  --T "${ASTER_T:-30}" \
  --n "${ASTER_N:-1.3}" \
  --V "${ASTER_V:-30}" \
  --max_spread "${ASTER_MAX_SPREAD:-0.2}" \
  --max_spread_ticks "${ASTER_MAX_SPREAD_TICKS:-2.0}" \
  --max_funding_abs_bps "${ASTER_MAX_FUNDING_ABS_BPS:-1.5}" \
  --taker_fee_bps "${ASTER_TAKER_FEE_BPS:-4.0}" \
  --take_profit_bps "${ASTER_TAKE_PROFIT_BPS:-20.0}" \
  --stop_loss_bps "${ASTER_STOP_LOSS_BPS:-12.0}" \
  --trailing_activation_bps "${ASTER_TRAILING_ACTIVATION_BPS:-8.0}" \
  --trailing_activation_buffer_bps "${ASTER_TRAILING_ACTIVATION_BUFFER_BPS:-0.5}" \
  --trailing_callback_bps "${ASTER_TRAILING_CALLBACK_BPS:-6.0}" \
  --min_take_profit_gap_bps "${ASTER_MIN_TAKE_PROFIT_GAP_BPS:-4.0}" \
  --margin_safety_multiple "${ASTER_MARGIN_SAFETY_MULTIPLE:-1.2}" \
  --daily_drawdown_blocker_pct "${ASTER_DAILY_DRAWDOWN_BLOCKER_PCT:-5.0}" \
  --reentry_cooldown_min "${ASTER_REENTRY_COOLDOWN_MIN:-10}" \
  --entry_halt_utc "${ASTER_ENTRY_HALT_UTC:-23:00}" \
  --force_exit_utc "${ASTER_FORCE_EXIT_UTC:-23:50}")

if [[ -n "${ASTER_ORDER_NOTIONAL:-}" ]]; then
  CMD+=(--order_notional "${ASTER_ORDER_NOTIONAL}")
fi
if [[ -n "${ASTER_CONFIG_CURRENT_FILE:-}" ]]; then
  CMD+=(--config_current_file "${ASTER_CONFIG_CURRENT_FILE}")
fi

# exec replaces shell with python process so systemd tracks true PID.
exec "${CMD[@]}"
