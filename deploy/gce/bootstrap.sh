#!/usr/bin/env bash
set -euo pipefail

# Deployment defaults:
# - APP_DIR is where code + venv + logs live on VM
# - APP_USER is the non-root runtime user for systemd
# - PYTHON_BIN allows pinning python version if needed
APP_DIR="${APP_DIR:-/opt/aster}"
APP_USER="${APP_USER:-aster}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CONNECTOR_REPO="${CONNECTOR_REPO:-https://github.com/asterdex/aster-connector-python.git}"
CONNECTOR_DIR="${CONNECTOR_DIR:-$APP_DIR/vendor/aster-connector-python}"

# Bootstrap does package install + user creation + venv provisioning,
# so it must run with elevated privileges.
if [[ "$EUID" -ne 0 ]]; then
  echo "Run as root: sudo bash deploy/gce/bootstrap.sh"
  exit 1
fi

# Install OS-level dependencies required by runtime scripts:
# python/venv/pip for app, git for deploy pulls, jq/curl for secret fetch.
apt-get update
apt-get install -y python3 python3-venv python3-pip git jq curl

# Create dedicated service account user if missing.
# Running trading code as non-root limits blast radius.
if ! id -u "$APP_USER" >/dev/null 2>&1; then
  useradd --create-home --shell /bin/bash "$APP_USER"
fi

# Create runtime directories:
# - logs for CSV outputs
# - .secrets for short-lived secret files
# - /var/log/aster reserved for optional file logs
mkdir -p "$APP_DIR" "$APP_DIR/logs" "$APP_DIR/.secrets" /var/log/aster
chown -R "$APP_USER:$APP_USER" "$APP_DIR" /var/log/aster
chmod 700 "$APP_DIR/.secrets"

# Guardrail: fail fast if repo is not present at APP_DIR.
if [[ ! -f "$APP_DIR/requirements.txt" ]]; then
  echo "Expected repo at $APP_DIR (missing requirements.txt). Clone/copy repo first."
  exit 1
fi

# Create project-local virtualenv once.
if [[ ! -d "$APP_DIR/.venv" ]]; then
  sudo -u "$APP_USER" "$PYTHON_BIN" -m venv "$APP_DIR/.venv"
fi

# Keep installer tooling fresh for faster/more reliable wheel installs.
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install --upgrade pip wheel

# Ensure local editable install of aster connector exists before installing
# requirements, so "aster-connector-python" is satisfied without PyPI.
if [[ ! -d "$CONNECTOR_DIR/.git" ]]; then
  sudo -u "$APP_USER" mkdir -p "$(dirname "$CONNECTOR_DIR")"
  sudo -u "$APP_USER" git clone "$CONNECTOR_REPO" "$CONNECTOR_DIR"
else
  sudo -u "$APP_USER" git -C "$CONNECTOR_DIR" fetch --all --tags
  sudo -u "$APP_USER" git -C "$CONNECTOR_DIR" pull --ff-only || true
fi
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install -e "$CONNECTOR_DIR"

# Prefer deploy-scoped requirements file so VM install can be curated.
# Fallback to repo root requirements if deploy override is absent.
REQ_FILE="$APP_DIR/deploy/gce/requirements.txt"
if [[ ! -f "$REQ_FILE" ]]; then
  REQ_FILE="$APP_DIR/requirements.txt"
fi
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install -r "$REQ_FILE"

# Ensure runtime scripts are executable for systemd/ops flows.
chmod +x \
  "$APP_DIR/deploy/gce/run_strategy.sh" \
  "$APP_DIR/deploy/gce/fetch_secrets.sh" \
  "$APP_DIR/deploy/gce/daily_batch_and_cleanup.sh"

# Print next manual steps after bootstrap completes.
echo "Bootstrap complete."
echo "Next:"
echo "1) cp $APP_DIR/deploy/gce/env.sample $APP_DIR/deploy/gce/aster.env"
echo "2) edit $APP_DIR/deploy/gce/aster.env"
echo "3) install systemd unit from deploy/gce/aster.service"
