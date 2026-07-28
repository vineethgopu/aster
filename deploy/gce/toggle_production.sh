#!/usr/bin/env bash
set -euo pipefail

ASTER_SERVICE="${ASTER_SERVICE:-aster.service}"
DAILY_STOP_TIMER="${DAILY_STOP_TIMER:-aster-daily-stop.timer}"
DAILY_RESTART_TIMER="${DAILY_RESTART_TIMER:-aster-daily-restart.timer}"
ENV_FILE="${ENV_FILE:-/opt/aster/deploy/gce/aster.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

usage() {
  cat <<'EOF'
Usage:
  sudo ./deploy/gce/toggle_production.sh on
  sudo ./deploy/gce/toggle_production.sh off
  sudo ./deploy/gce/toggle_production.sh status

Actions:
  on      Enable + start runtime service and daily timers.
  off     Disable + stop daily timers and runtime service.
  status  Show enabled/active state for service and timers.
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Run with sudo (root required for systemctl enable/disable)."
    exit 1
  fi
}

print_unit_state() {
  local unit="$1"
  local enabled active
  enabled="$(systemctl is-enabled "$unit" 2>/dev/null || true)"
  active="$(systemctl is-active "$unit" 2>/dev/null || true)"
  echo "  - ${unit}: enabled=${enabled:-unknown}, active=${active:-unknown}"
}

status() {
  echo "[TOGGLE_PRODUCTION] Current unit states:"
  print_unit_state "$ASTER_SERVICE"
  print_unit_state "$DAILY_STOP_TIMER"
  print_unit_state "$DAILY_RESTART_TIMER"
  echo
  echo "[TOGGLE_PRODUCTION] Timer schedule:"
  systemctl list-timers --all | grep -E "aster-daily-stop|aster-daily-restart|NEXT|LEFT" || true
}

on() {
  require_root
  systemctl daemon-reload
  systemctl enable --now "$ASTER_SERVICE"
  local daily_timers_enabled
  daily_timers_enabled="$(printf '%s' "${ASTER_ENABLE_DAILY_TIMERS:-true}" | tr '[:upper:]' '[:lower:]')"
  if [[ "$daily_timers_enabled" == "true" ]]; then
    systemctl enable --now "$DAILY_STOP_TIMER" "$DAILY_RESTART_TIMER"
  else
    systemctl disable --now "$DAILY_STOP_TIMER" "$DAILY_RESTART_TIMER" || true
    echo "[TOGGLE_PRODUCTION] Daily stop/restart timers disabled by ASTER_ENABLE_DAILY_TIMERS=false."
  fi
  echo "[TOGGLE_PRODUCTION] Production mode enabled."
  status
}

off() {
  require_root
  systemctl disable --now "$DAILY_STOP_TIMER" "$DAILY_RESTART_TIMER"
  systemctl disable --now "$ASTER_SERVICE"
  echo "[TOGGLE_PRODUCTION] Production mode disabled."
  status
}

cmd="${1:-status}"
case "$cmd" in
  on) on ;;
  off) off ;;
  status) status ;;
  -h|--help|help) usage ;;
  *)
    echo "Unknown command: $cmd"
    usage
    exit 2
    ;;
esac
