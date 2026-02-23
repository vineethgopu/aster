#!/usr/bin/env bash
set -euo pipefail

# Daily 00:00 UTC maintenance/update workflow:
# 1) pull latest code
# 2) run bootstrap to refresh venv/runtime wiring
# 3) install requirements.txt explicitly
# 4) sync systemd units
# 5) daemon-reload + restart aster

APP_DIR="${APP_DIR:-/opt/aster}"
ENV_FILE="${ENV_FILE:-$APP_DIR/deploy/gce/aster.env}"
APP_USER="${APP_USER:-aster}"

if [[ "$EUID" -ne 0 ]]; then
  echo "Run as root (systemd oneshot): sudo bash $APP_DIR/deploy/gce/daily_update_and_restart.sh"
  exit 1
fi

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "[DAILY_RESTART] Missing git repo at $APP_DIR"
  exit 1
fi

# Capture current git metadata owner so bootstrap's broad chown does not
# permanently block your deploy user from manual git operations.
GIT_UID="$(stat -c '%u' "$APP_DIR/.git" 2>/dev/null || echo 0)"
GIT_GID="$(stat -c '%g' "$APP_DIR/.git" 2>/dev/null || echo 0)"

echo "[DAILY_RESTART] Pulling latest repo changes..."
git -C "$APP_DIR" -c safe.directory="$APP_DIR" pull --ff-only

echo "[DAILY_RESTART] Running bootstrap..."
bash "$APP_DIR/deploy/gce/bootstrap.sh"

if [[ ! -x "$APP_DIR/.venv/bin/pip" ]]; then
  echo "[DAILY_RESTART] Missing venv pip: $APP_DIR/.venv/bin/pip"
  exit 1
fi

echo "[DAILY_RESTART] Installing Python dependencies from requirements.txt..."
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

echo "[DAILY_RESTART] Restoring git metadata ownership..."
chown -R "$GIT_UID:$GIT_GID" "$APP_DIR/.git" || true

# Keep safe.directory on root profile to avoid git safety blocks in this unit.
git config --global --add safe.directory "$APP_DIR" || true

echo "[DAILY_RESTART] Syncing systemd unit files..."
cp "$APP_DIR/deploy/gce/aster.service" /etc/systemd/system/aster.service
cp "$APP_DIR/deploy/gce/aster-daily-stop.service" /etc/systemd/system/aster-daily-stop.service
cp "$APP_DIR/deploy/gce/aster-daily-stop.timer" /etc/systemd/system/aster-daily-stop.timer
cp "$APP_DIR/deploy/gce/aster-daily-restart.service" /etc/systemd/system/aster-daily-restart.service
cp "$APP_DIR/deploy/gce/aster-daily-restart.timer" /etc/systemd/system/aster-daily-restart.timer
cp "$APP_DIR/deploy/gce/backtest/aster-backtest.service" /etc/systemd/system/aster-backtest.service
cp "$APP_DIR/deploy/gce/backtest/aster-backtest.timer" /etc/systemd/system/aster-backtest.timer
cp "$APP_DIR/deploy/gce/aster-email-prod.service" /etc/systemd/system/aster-email-prod.service
cp "$APP_DIR/deploy/gce/aster-email-prod.timer" /etc/systemd/system/aster-email-prod.timer

echo "[DAILY_RESTART] Reloading systemd and restarting aster..."
systemctl daemon-reload
systemctl restart aster.service
systemctl status aster.service --no-pager || true

echo "[DAILY_RESTART] Completed."
