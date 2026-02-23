# Aster Mid-Frequency Futures Strategy

This repository contains:
- A live data collection + strategy execution stack for Aster perpetual futures
- A backtest/feature pipeline for offline parameter search

## Repository Layout

```text
.
├── core/
│   ├── client.py
│   ├── logs.py
│   ├── main.py
│   ├── order.py
│   └── strategy.py
├── logs/
│   ├── kline_YYYYMMDD.csv
│   ├── bookTicker_YYYYMMDD.csv
│   ├── markPrice_YYYYMMDD.csv
│   ├── aggTrade_1s_YYYYMMDD.csv
│   └── depth5_YYYYMMDD.csv
├── backtest/
│   ├── build_backtest_inputs.py
│   ├── backtest.py
│   ├── config_grid.json
│   └── backtest_inputs.csv
├── deploy/
│   └── gce/
│       ├── aster-daily-restart.service
│       ├── aster-daily-restart.timer
│       ├── aster-daily-stop.service
│       ├── aster-daily-stop.timer
│       ├── aster-email-prod.service
│       ├── aster-email-prod.timer
│       ├── aster.service
│       ├── bootstrap.sh
│       ├── bq_load_logs.py
│       ├── cleanup_old_logs.py
│       ├── daily_batch_and_cleanup.sh
│       ├── email_daily_report.py
│       ├── env.sample
│       ├── fetch_secrets.sh
│       ├── requirements.txt
│       ├── run_strategy.sh
│       ├── toggle_production.sh
│       └── backtest/
│           ├── aster-backtest.service
│           ├── aster-backtest.timer
│           └── run_backtest.sh
├── requirements.txt
└── .vscode/launch.json
```

## Core Modules

### `core/client.py`
Purpose:
- Connects to Aster REST + WebSocket
- Seeds initial caches via REST snapshot
- Maintains live in-memory market caches per symbol

Caches maintained:
- `latest_kline_1m`
- `latest_bbo`
- `latest_funding`
- `recent_agg_trades`
- `latest_l2`

Data sources:
- REST startup snapshot:
  - klines, bookTicker, markPrice, aggTrades, depth
- WebSocket streams:
  - `kline_1m`
  - `bookTicker`
  - `markPrice@1s`
  - `aggTrade`
  - `depth5@100ms`

Shutdown:
- Graceful close handling to reduce reconnect/1006 shutdown noise

### `core/logs.py`
Purpose:
- Buffered CSV logging manager for market snapshots

Outputs:
- `kline_YYYYMMDD.csv`
- `bookTicker_YYYYMMDD.csv`
- `markPrice_YYYYMMDD.csv`
- `aggTrade_1s_YYYYMMDD.csv`
- `depth5_YYYYMMDD.csv`

Notes:
- Supports optional deletion of existing CSVs on startup
- Writes header once; buffered flush
- Auto-rolls files by UTC date so each day lands in a separate CSV

### `core/strategy.py`
Purpose:
- Pure signal logic from cached market snapshots

Entry signal requires:
1. Indicator 1 (momentum-volatility):
   - `ret_bps > k * RS_vol(T)` => long
   - `ret_bps < -k * RS_vol(T)` => short
2. Indicator 2 (volume regime):
   - `bar_volume > n * rolling_avg_volume(V)`
3. Blockers pass:
   - spread <= max spread
   - |funding| <= max funding bps
   - opening loss <= min(10, max(5, 2 * spread_bps))

Warm-up behavior:
- No trade signal until both rolling windows are fully populated (`T` and `V`)

### `core/order.py`
Purpose:
- Order entry/exit wrappers around `aster.rest_api.Client`

Entry logic:
- Taker-only IOC LIMIT at touch
  - BUY at ask
  - SELL at bid
- Quantity/price normalization with exchange filters from `exchange_info`
  - `PRICE_FILTER.tickSize`
  - `LOT_SIZE.stepSize/minQty/maxQty`
  - `MIN_NOTIONAL/NOTIONAL`

Sizing:
- Computes quantity from `order_notional` (USD) such that notional is at least target

Exit logic (armed after fill):
- `TAKE_PROFIT_MARKET` (CONTRACT_PRICE trigger)
- `STOP_MARKET` (MARK_PRICE trigger)
- `TRAILING_STOP_MARKET` (activation + callback)
- `reduceOnly=True` for all exit triggers
- Trigger levels are defined in bps from entry price (not multiplier/fraction config)
- Trailing activation price is computed from `trailing_activation_bps`
- Sibling exit cleanup: when position is detected flat, remaining TP/SL/trailing orders are cancelled

Risk controls:
- Margin kill-switch based on safety multiple:
  - `safety_multiple = totalMarginBalance / totalMaintMargin`
  - force close when `<= threshold` (default 1.2)

Startup account setup helper:
- `ensure_risk_setup(symbols, leverage=10, margin_type="ISOLATED")`

Cleanup helper:
- `cancel_sibling_exit_orders(pos)` to cancel remaining TP/SL/trailing when position is flat

### `core/main.py`
Purpose:
- Runtime orchestrator for polling, strategy evaluation, and optional live trading

Flow:
1. Parse CLI params
2. Create `AsterClient`, `Strategy`, optional `OrderPlacer`
3. If trading enabled, apply leverage + margin setup for all symbols
4. Start WS subscriptions
5. Every second:
   - build symbol snapshot from client caches
   - optionally write CSV logs (`--update_logs`)
   - run strategy on closed 1m bars
   - if signal triggered and no open local position:
     - compute TP/SL/trailing params in bps
       - breakeven floor:
         - `be_floor_bps = 2 * taker_fee_bps + opening_loss_bps + |funding_bps|/8`
       - activation floor:
         - `activation_bps = max(trailing_activation_bps, be_floor_bps + trailing_activation_buffer_bps)`
       - TP floor:
         - `take_profit_bps = max(take_profit_bps, activation_bps + min_take_profit_gap_bps)`
     - compute order qty from `order_notional`
     - submit entry
     - arm exits
   - if position exists:
     - check if flat on exchange, cleanup siblings
     - do not re-enter position
     - run margin kill check
   - after exit: complete cooldown period before entry signal is functional

## Backtest Modules

### `backtest/build_backtest_inputs.py`
Purpose:
- Compile backtest feature table from BigQuery tables

Inputs:
- `<PROJECT_ID>.aster.kline`
- `<PROJECT_ID>.aster.book_ticker`
- `<PROJECT_ID>.aster.mark_price`
- filtered by `--symbols`, `--start_date`, `--end_date`

Feature engineering:
- Uses last snapshot per minute per symbol (backtest-friendly, does not require `k1_closed=True`)
- Computes:
  - `open`, `high`, `low`, `close`, `vol1m`, `ret_bps`
  - `spread_bps`
  - `funding_bps`
  - opening loss proxies
  - time-weighted quote fields:
    - `tw_bid_px`, `tw_ask_px` (1-second forward-filled then minute-averaged)
  - `rs_var_1m`
  - rolling `rs_vol_{10,30,60}m_bps`
  - rolling `avg_vol_{10,30,60}m`

### `backtest/backtest.py`
Purpose:
- VectorBT-based parameter sweep and ranking using `backtest/config_grid.json`

Main components:
- Builds per-symbol parameter cartesian products from nested config
- Enforces required config keys for each symbol:
  - `k, T, n, V, tp_bps, sl_bps, activation_bps, activation_buffer_bps, callback_bps, min_tp_gap_bps, spread_max, funding_max`
- `build_signals`: vectorized long/short entries + exits across all configs
- `run_grid_backtest`: `vbt.Portfolio.from_signals(...)` with:
  - intrabar stop evaluation using `open/high/low`
  - execution price anchored to `tw_mid = (tw_bid_px + tw_ask_px)/2`
  - per-order slippage = half-spread + configured extra slippage
  - stop pricing with `stop_entry_price="Price"` and `stop_exit_price="Price"`
  - trailing activation emulation via `adjust_sl_func_nb`:
    - stay on base SL until activation threshold is reached
    - then switch to trailing callback distance
- `build_ranked_metrics`: PnL/trade/risk metrics table (ranked by total_pnl)
- Outputs:
  - `ranked_metrics.csv` (metrics + config_id + param values)
  - `config_mapping.csv` (config_id -> symbol + params)

## Typical Commands

### Live run (dry mode, no orders)
```bash
python core/main.py \
  --symbols BTCUSDT,ETHUSDT \
  --poll_time 600 \
  --enable_trading false \
  --update_logs true
```

### Live run (with trading)
```bash
export ORDER_API_KEY=./api/api.txt
export ORDER_SECRET_KEY=./api/secret.txt

python core/main.py \
  --symbols ETHUSDT \
  --poll_time 600 \
  --enable_trading true \
  --order_notional 5
```

### Build backtest features from BigQuery
```bash
python backtest/build_backtest_inputs.py \
  --project <PROJECT_ID> \
  --dataset aster \
  --symbols ETHUSDT,BTCUSDT \
  --start_date 2026-02-01 \
  --end_date 2026-02-28 \
  --out_csv ./backtest/backtest_inputs.csv \
  --windows 10,30,60
```

### Run vectorized backtest sweep (config-driven)
```bash
python backtest/backtest.py \
  --config_file ./backtest/config_grid.json \
  --inputs_csv ./backtest/backtest_inputs.csv \
  --out_config_map_csv ./backtest/results/config_mapping.csv \
  --out_ranked_csv ./backtest/results/ranked_metrics.csv
```

## GCE Deployment (VM + systemd)

### 1) Create local gcloud CLI context
```bash
# macOS (Homebrew)
brew install python@3.13
echo 'export CLOUDSDK_PYTHON=/opt/homebrew/opt/python@3.13/libexec/bin/python' >> ~/.zshrc
source ~/.zshrc
brew reinstall --cask gcloud-cli

gcloud auth login
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
gcloud config set compute/zone <ZONE>
```

### 2) Create/refresh secrets in Secret Manager
```bash
printf '%s' '<ASTER_API_KEY>' | gcloud secrets create aster-api-key \
  --replication-policy=automatic --data-file=- || true
printf '%s' '<ASTER_API_KEY>' | gcloud secrets versions add aster-api-key --data-file=-

printf '%s' '<ASTER_API_SECRET>' | gcloud secrets create aster-api-secret \
  --replication-policy=automatic --data-file=- || true
printf '%s' '<ASTER_API_SECRET>' | gcloud secrets versions add aster-api-secret --data-file=-
```

### 2.1) Runtime service account IAM (required for batch + logs)

Attach these roles to the VM runtime service account:
- `roles/secretmanager.secretAccessor`
- `roles/logging.logWriter`
- `roles/bigquery.jobUser`
- `roles/bigquery.dataEditor` (dataset-level preferred)

### 2.2) Create BigQuery dataset + tables manually (required)

`deploy/gce/bq_load_logs.py` only loads data. It does not create datasets, tables, partitions, or clusters.

Run this SQL in BigQuery editor (replace `<PROJECT_ID>`):

```sql
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.aster`
OPTIONS(location = 'US');

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.aster.kline` (
  ts_unix_ms INT64,
  ts_dt_utc STRING,
  symbol STRING,
  k1_start_ms INT64,
  k1_close_ms INT64,
  k1_open FLOAT64,
  k1_high FLOAT64,
  k1_low FLOAT64,
  k1_close FLOAT64,
  k1_base_vol FLOAT64,
  k1_quote_vol FLOAT64,
  k1_trades INT64,
  k1_closed BOOL,
  date DATE,
  hour INT64,
  minute INT64,
  second INT64
)
PARTITION BY date
CLUSTER BY symbol, hour, minute, second;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.aster.book_ticker` (
  ts_unix_ms INT64,
  ts_dt_utc STRING,
  symbol STRING,
  bid_px FLOAT64,
  bid_qty FLOAT64,
  ask_px FLOAT64,
  ask_qty FLOAT64,
  spread FLOAT64,
  mid FLOAT64,
  imbalance FLOAT64,
  weighted_mid FLOAT64,
  date DATE,
  hour INT64,
  minute INT64,
  second INT64
)
PARTITION BY date
CLUSTER BY symbol, hour, minute, second;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.aster.mark_price` (
  ts_unix_ms INT64,
  ts_dt_utc STRING,
  symbol STRING,
  mark_px FLOAT64,
  index_px FLOAT64,
  funding_rate FLOAT64,
  next_funding_time_ms INT64,
  mark_index_bps FLOAT64,
  date DATE,
  hour INT64,
  minute INT64,
  second INT64
)
PARTITION BY date
CLUSTER BY symbol, hour, minute, second;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.aster.agg_trade_1s` (
  ts_unix_ms INT64,
  ts_dt_utc STRING,
  symbol STRING,
  n_trades_1s INT64,
  sum_qty_1s FLOAT64,
  vwap_1s FLOAT64,
  buy_qty_1s FLOAT64,
  sell_qty_1s FLOAT64,
  buy_notional_1s FLOAT64,
  sell_notional_1s FLOAT64,
  date DATE,
  hour INT64,
  minute INT64,
  second INT64
)
PARTITION BY date
CLUSTER BY symbol, hour, minute, second;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.aster.depth5` (
  ts_unix_ms INT64,
  ts_dt_utc STRING,
  symbol STRING,
  bid1_px FLOAT64,
  bid2_px FLOAT64,
  bid3_px FLOAT64,
  bid4_px FLOAT64,
  bid5_px FLOAT64,
  bid1_qty FLOAT64,
  bid2_qty FLOAT64,
  bid3_qty FLOAT64,
  bid4_qty FLOAT64,
  bid5_qty FLOAT64,
  ask1_px FLOAT64,
  ask2_px FLOAT64,
  ask3_px FLOAT64,
  ask4_px FLOAT64,
  ask5_px FLOAT64,
  ask1_qty FLOAT64,
  ask2_qty FLOAT64,
  ask3_qty FLOAT64,
  ask4_qty FLOAT64,
  ask5_qty FLOAT64,
  obi5 FLOAT64,
  date DATE,
  hour INT64,
  minute INT64,
  second INT64
)
PARTITION BY date
CLUSTER BY symbol, hour, minute, second;
```

### 3) Prepare VM and app runtime
```bash
gcloud compute ssh <VM_NAME> --zone <ZONE>

# On VM
sudo mkdir -p /opt
sudo chown -R "$USER":"$USER" /opt
cd /opt
if [ ! -d /opt/aster/.git ]; then
  git clone <REPO_URL> aster
fi
cd /opt/aster
git pull --ff-only

sudo bash deploy/gce/bootstrap.sh
sudo cp deploy/gce/env.sample deploy/gce/aster.env
sudo chown aster:aster deploy/gce/aster.env
sudo chmod 640 deploy/gce/aster.env
sudo nano deploy/gce/aster.env

# Daily trading schedule (UTC):
# - No new entries after ASTER_ENTRY_HALT_UTC (default 23:00)
# - Force-close positions after ASTER_FORCE_EXIT_UTC (default 23:50)
# These defaults align with the optional daily maintenance timers below.

# Daily data pipeline:
# - ASTER_BQ_ENABLE_DAILY_BATCH=true
# - ASTER_BQ_PROJECT=<PROJECT_ID> (optional; defaults from VM metadata)
# - ASTER_BQ_DATASET=aster
# - ASTER_LOG_RETENTION_DAYS=7

# bootstrap chowns /opt/aster to service user (aster). Reclaim git metadata
# ownership for your SSH user to avoid "dubious ownership" on future pulls.
sudo chown -R "$USER":"$USER" /opt/aster/.git
git config --global --add safe.directory /opt/aster
```

### 4) Install and start systemd service
```bash
sudo cp /opt/aster/deploy/gce/aster.service /etc/systemd/system/aster.service
sudo systemctl daemon-reload
sudo systemctl enable aster
sudo systemctl start aster
```

### 4.1) Enable daily maintenance window (optional)

This schedules:
- Batch window at `23:55 UTC`:
  - stop `aster.service`
  - load same-day CSVs into BigQuery
  - delete local CSVs older than 7 days
- Restart at `00:00 UTC`

```bash
sudo cp /opt/aster/deploy/gce/aster-daily-stop.service /etc/systemd/system/aster-daily-stop.service
sudo cp /opt/aster/deploy/gce/aster-daily-stop.timer /etc/systemd/system/aster-daily-stop.timer
sudo cp /opt/aster/deploy/gce/aster-daily-restart.service /etc/systemd/system/aster-daily-restart.service
sudo cp /opt/aster/deploy/gce/aster-daily-restart.timer /etc/systemd/system/aster-daily-restart.timer

sudo systemctl daemon-reload
sudo systemctl enable --now aster-daily-stop.timer aster-daily-restart.timer

systemctl list-timers --all | grep aster
```

Notes:
- `aster.service` uses `Restart=on-failure`, so a clean exit after `ASTER_POLL_TIME` will not auto-restart.
  - For continuous runtime, set `ASTER_POLL_TIME` to a large value (e.g. `86400`) and rely on the daily timers.
- BigQuery dataset/tables must exist before batch load (create once via SQL above).
- BigQuery does not have traditional row indexes; this pipeline uses:
  - `PARTITION BY date`
  - `CLUSTER BY symbol, hour, minute, second`

### 5) Operate and verify
```bash
sudo systemctl status aster
journalctl -u aster -f
ls -lh /opt/aster/logs
```

Manual secret fetch test (without running service):
```bash
cd /opt/aster
sudo bash -lc 'set -a; source /opt/aster/deploy/gce/aster.env; set +a; /opt/aster/deploy/gce/fetch_secrets.sh'
```

Manual daily batch test (stop + upload + cleanup):
```bash
cd /opt/aster
sudo /opt/aster/deploy/gce/daily_batch_and_cleanup.sh
```

Manual BigQuery load test only (no writes):
```bash
cd /opt/aster
/opt/aster/.venv/bin/python /opt/aster/deploy/gce/bq_load_logs.py \
  --project <PROJECT_ID> \
  --log_dir /opt/aster/logs \
  --dataset aster \
  --date "$(date -u +%Y%m%d)" \
  --dry_run
```

Toggle full production automation (runtime + daily timers):
```bash
sudo /opt/aster/deploy/gce/toggle_production.sh on
sudo /opt/aster/deploy/gce/toggle_production.sh off
sudo /opt/aster/deploy/gce/toggle_production.sh status
```

### 5.1) Enable weekly backtest timer (optional)

This schedules a weekly backtest run at `Sun 00:20 UTC`:
- Builds `backtest_inputs.csv` from BigQuery
- Runs config-driven VectorBT sweep
- Writes:
  - `/opt/aster/backtest/results/config_mapping.csv`
  - `/opt/aster/backtest/results/ranked_metrics.csv`

Defaults in `run_backtest.sh`:
- if `ASTER_BACKTEST_START_DATE`/`ASTER_BACKTEST_END_DATE` are empty:
  - `end_date = UTC yesterday` (Saturday for the scheduled Sunday run)
  - `start_date = UTC 28 days ago` (Sunday, 4 weeks before `end_date`)

```bash
sudo cp /opt/aster/deploy/gce/backtest/aster-backtest.service /etc/systemd/system/aster-backtest.service
sudo cp /opt/aster/deploy/gce/backtest/aster-backtest.timer /etc/systemd/system/aster-backtest.timer
sudo systemctl daemon-reload
sudo systemctl enable --now aster-backtest.timer

systemctl list-timers --all | grep aster-backtest
```

Manual one-off run:
```bash
sudo /opt/aster/deploy/gce/backtest/run_backtest.sh
journalctl -u aster-backtest -n 200 --no-pager
```

### 5.2) Enable email reports (optional)

Email report modes:
- Daily production report (`00:35 UTC` via `aster-email-prod.timer`):
  - `aster` service status
  - trades per symbol (today UTC) with realized PnL stats
  - runtime issue metadata from logs (errors/warnings/timeouts/connection closes/tracebacks)
- Backtest completion report (triggered by `run_backtest.sh` on successful completion):
  - `aster-backtest` service status
  - top-10 configs per symbol from `ranked_metrics.csv`

Configure in `/opt/aster/deploy/gce/aster.env`:
```bash
ASTER_EMAIL_PROD_ENABLE=true
ASTER_EMAIL_BACKTEST_ENABLE=true
ASTER_EMAIL_BACKTEST_ON_COMPLETION=true
ASTER_EMAIL_SMTP_HOST=<smtp_host>
ASTER_EMAIL_SMTP_PORT=587
ASTER_EMAIL_SMTP_USER=<smtp_user>
ASTER_EMAIL_SMTP_PASS=<smtp_pass>
ASTER_EMAIL_FROM=<from_email>
ASTER_EMAIL_TO_PROD=<prod_recipients_csv>
ASTER_EMAIL_TO_BACKTEST=<backtest_recipients_csv>
```

Install units/timers:
```bash
sudo cp /opt/aster/deploy/gce/aster-email-prod.service /etc/systemd/system/aster-email-prod.service
sudo cp /opt/aster/deploy/gce/aster-email-prod.timer /etc/systemd/system/aster-email-prod.timer
sudo systemctl daemon-reload
sudo systemctl enable --now aster-email-prod.timer
```

Manual tests:
```bash
sudo systemctl start aster-email-prod.service
sudo -u aster /opt/aster/.venv/bin/python /opt/aster/deploy/gce/email_daily_report.py --mode backtest
journalctl -u aster-email-prod -n 200 --no-pager
```

### 6) Update VM when repo changes

Assumption:
- First-time setup already completed, and repo already exists at `/opt/aster`.

Start every update from your VM checkout:
```bash
gcloud compute ssh <VM_NAME> --zone <ZONE>
cd /opt/aster
git status
git fetch --all --tags
git pull --ff-only
```

#### 6.1) Python code changes only
Use this when only `.py` files changed and `requirements.txt` did not change.
```bash
cd /opt/aster
git pull --ff-only
sudo systemctl restart aster
sudo systemctl status aster --no-pager
journalctl -u aster -n 100 --no-pager
```

#### 6.2) Python dependency changes
Use this when `requirements.txt` or `deploy/gce/requirements.txt` changed.
```bash
cd /opt/aster
git pull --ff-only
/opt/aster/.venv/bin/pip install --upgrade pip wheel
/opt/aster/.venv/bin/pip install -r /opt/aster/deploy/gce/requirements.txt
sudo systemctl restart aster
sudo systemctl status aster --no-pager
```

#### 6.3) OS/runtime environment changes
Use this when deployment scripts or service wiring changed:
- `deploy/gce/bootstrap.sh`
- `deploy/gce/bq_load_logs.py`
- `deploy/gce/cleanup_old_logs.py`
- `deploy/gce/daily_batch_and_cleanup.sh`
- `deploy/gce/fetch_secrets.sh`
- `deploy/gce/run_strategy.sh`
- `deploy/gce/toggle_production.sh`
- `deploy/gce/backtest/run_backtest.sh`
- `deploy/gce/email_daily_report.py`
- `deploy/gce/aster.service`
- `deploy/gce/aster-daily-*.service` / `deploy/gce/aster-daily-*.timer`
- `deploy/gce/backtest/aster-backtest.service`
- `deploy/gce/backtest/aster-backtest.timer`
- `deploy/gce/aster-email-prod.service` / `deploy/gce/aster-email-prod.timer`

```bash
cd /opt/aster
git pull --ff-only
sudo bash /opt/aster/deploy/gce/bootstrap.sh

# bootstrap resets ownership to service user; restore git metadata ownership
# for your deploy user before the next git operation.
sudo chown -R "$USER":"$USER" /opt/aster/.git
git config --global --add safe.directory /opt/aster

sudo cp /opt/aster/deploy/gce/aster.service /etc/systemd/system/aster.service
sudo cp /opt/aster/deploy/gce/aster-daily-stop.service /etc/systemd/system/aster-daily-stop.service
sudo cp /opt/aster/deploy/gce/aster-daily-stop.timer /etc/systemd/system/aster-daily-stop.timer
sudo cp /opt/aster/deploy/gce/aster-daily-restart.service /etc/systemd/system/aster-daily-restart.service
sudo cp /opt/aster/deploy/gce/aster-daily-restart.timer /etc/systemd/system/aster-daily-restart.timer
sudo cp /opt/aster/deploy/gce/backtest/aster-backtest.service /etc/systemd/system/aster-backtest.service
sudo cp /opt/aster/deploy/gce/backtest/aster-backtest.timer /etc/systemd/system/aster-backtest.timer
sudo cp /opt/aster/deploy/gce/aster-email-prod.service /etc/systemd/system/aster-email-prod.service
sudo cp /opt/aster/deploy/gce/aster-email-prod.timer /etc/systemd/system/aster-email-prod.timer
sudo systemctl daemon-reload
sudo systemctl restart aster
sudo systemctl status aster --no-pager
```

If only runtime params changed (`deploy/gce/aster.env`), just edit env and restart:
```bash
sudo nano /opt/aster/deploy/gce/aster.env
sudo systemctl restart aster
```

#### 6.4) Other important update flows

Secret rotation:
```bash
gcloud secrets versions add aster-api-key --data-file=api/api.txt
gcloud secrets versions add aster-api-secret --data-file=api/secret.txt
gcloud compute ssh <VM_NAME> --zone <ZONE> --command "sudo systemctl restart aster"
```

Rollback to known-good commit/tag:
```bash
gcloud compute ssh <VM_NAME> --zone <ZONE>
cd /opt/aster
git log --oneline -n 20
git checkout <commit_or_tag>
sudo systemctl restart aster
```

Post-update health checks:
```bash
sudo systemctl status aster --no-pager
journalctl -u aster -n 200 --no-pager
ls -lh /opt/aster/logs
```

## BigQuery Adhoc Queries (Jupyter)

Install notebook deps in your local env:
```bash
pip install google-cloud-bigquery pandas-gbq
```

Example notebook code:
```python
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="<PROJECT_ID>")

sql = """
SELECT
  date,
  hour,
  minute,
  second,
  symbol,
  ts_unix_ms,
  k1_close,
  k1_base_vol
FROM `<PROJECT_ID>.aster.kline`
WHERE date BETWEEN DATE('2026-02-20') AND DATE('2026-02-22')
  AND symbol = 'ETHUSDT'
ORDER BY ts_unix_ms
LIMIT 5000
"""

df = client.query(sql).to_dataframe()
df.head()
```

Aggregate query example:
```python
sql = """
SELECT
  date,
  symbol,
  COUNT(*) AS n_rows,
  AVG(spread) AS avg_spread,
  AVG(mid) AS avg_mid
FROM `<PROJECT_ID>.aster.book_ticker`
WHERE date = DATE('2026-02-22')
GROUP BY date, symbol
ORDER BY symbol
"""
pd.read_gbq(sql, project_id="<PROJECT_ID>")
```

## Notes

- Paths like `./logs` are relative to your command working directory.
- Keep API credentials out of version control.
- Live logs are written as dated files (for example `kline_20260222.csv`).
- Daily batch at `23:55 UTC` loads logs into BigQuery and removes local files older than 7 days.
- BigQuery tables are managed manually (the loader only validates and writes).
- Live and backtest flows are intentionally separated:
  - `core/*` for streaming + execution
  - `backtest/*` for offline evaluation
