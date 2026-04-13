---
name: aster-gcp-ops
description: Use when working on GCE deployment, systemd services or timers, Secret Manager, BigQuery log or backtest loaders, or operational runbooks for the Aster strategy.
---

# Aster GCP Ops

## When to use

- Changes under `deploy/gce/`
- Questions about VM bootstrap, runtime wiring, timers, secrets, BigQuery, or reporting
- Operational debugging for production and backtest jobs

## Deployment model

- Primary deployment target is a single GCE VM with repo checkout at `/opt/aster`.
- Runtime user is `aster`.
- `deploy/gce/bootstrap.sh` must run as root and does the heavy setup:
  - installs OS dependencies
  - creates the service user
  - provisions `.venv`
  - clones `aster-connector-python`
  - installs Python requirements
  - marks runtime scripts executable

## Runtime start path

- `deploy/gce/aster.service` runs `deploy/gce/run_strategy.sh`
- `run_strategy.sh`:
  - loads `deploy/gce/aster.env`
  - fetches fresh secrets
  - exports file-based API secret paths for `core/main.py`
  - activates `.venv`
  - changes into `/opt/aster/core`
  - executes `python main.py ...`

## Secrets

- `deploy/gce/fetch_secrets.sh` uses the GCE metadata server to fetch:
  - project ID
  - short-lived OAuth token from the attached VM service account
- It then reads Secret Manager values into `ASTER_SECRET_DIR`
- Required runtime secrets are:
  - API key
  - API secret
  - SMTP password

## Timers and services

- `aster.service`: live strategy runtime
- `aster-daily-stop.timer`: 23:55 UTC daily batch and cleanup window
- `aster-daily-restart.timer`: 00:00 UTC daily git pull, bootstrap, and restart
- `aster-email-prod.timer`: 00:35 UTC production report
- `backtest/aster-backtest.timer`: Sunday 00:20 UTC backtest run

## BigQuery

- Daily log loader: `deploy/gce/bq_load_logs.py`
  - expects dataset and tables to already exist
  - deletes the target `date` partition and appends CSV rows
- Backtest loader: `deploy/gce/backtest/bq_load_backtest_outputs.py`
  - uploads symbol-dated result and config files
  - also expects tables to already exist
- Current backtest feature builder reads:
  - `kline`
  - `book_ticker`
  - `mark_price`
- Daily log upload also handles:
  - `agg_trade_1s`
  - `depth5`
  - `orders`

## Important env knobs

- Trading and runtime:
  - `ASTER_ENABLE_TRADING`
  - `ASTER_SYMBOLS`
  - `ASTER_CONFIG_CURRENT_FILE`
  - `ASTER_TARGET_LEVERAGE`
  - `ASTER_RISK_PCT`
- Risk windows:
  - `ASTER_ENTRY_HALT_UTC`
  - `ASTER_FORCE_EXIT_UTC`
  - `ASTER_DAILY_DRAWDOWN_BLOCKER_PCT`
- BigQuery:
  - `ASTER_BQ_ENABLE_DAILY_BATCH`
  - `ASTER_BQ_PROJECT`
  - `ASTER_BQ_DATASET`
  - `ASTER_BQ_LOCATION`
- Backtests:
  - `ASTER_BACKTEST_*`
- Email:
  - `ASTER_EMAIL_*`

## Change guidance

- Treat deployment scripts as ops-critical. Preserve idempotency, fail-fast checks, and ownership assumptions.
- If you change CSV schema or filenames, also update BigQuery loaders, table expectations, and reporting flows.
- If a change touches timers or systemd units, check the full schedule:
  - stop and batch at 23:55 UTC
  - restart at 00:00 UTC
  - production email at 00:35 UTC
  - weekly backtest at Sunday 00:20 UTC
- BigQuery table creation is manual today; loaders do not create datasets, tables, partitions, or clusters.
