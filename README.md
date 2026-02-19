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
│   ├── kline.csv
│   ├── bookTicker.csv
│   ├── markPrice.csv
│   ├── aggTrade_1s.csv
│   └── depth5.csv
├── backtest/
│   ├── build_backtest_inputs.py
│   ├── backtest.py
│   └── backtest_inputs.csv
├── deploy/
│   └── gce/
│       ├── aster.service
│       ├── bootstrap.sh
│       ├── env.sample
│       ├── fetch_secrets.sh
│       ├── requirements.txt
│       └── run_strategy.sh
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
- `kline.csv`
- `bookTicker.csv`
- `markPrice.csv`
- `aggTrade_1s.csv`
- `depth5.csv`

Notes:
- Supports optional deletion of existing CSVs on startup
- Writes header once; buffered flush

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
     - compute TP/SL/trailing params
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
- Compile backtest feature table from CSV logs

Inputs:
- `logs/kline.csv`
- `logs/bookTicker.csv`
- `logs/markPrice.csv`

Feature engineering:
- Uses last snapshot per minute per symbol (backtest-friendly, does not require `k1_closed=True`)
- Computes:
  - `close`, `vol1m`, `ret_bps`
  - `spread_bps`
  - `funding_bps`
  - opening loss proxies
  - `rs_var_1m`
  - rolling `rs_vol_{10,30,60}m_bps`
  - rolling `avg_vol_{10,30,60}m`

### `backtest/backtest.py`
Purpose:
- VectorBT-based parameter sweep and ranking

Main components:
- `make_param_grid`: cartesian product of parameter sets
- `build_signals`: vectorized long/short entries + exits across all configs
- `run_grid_backtest`: `vbt.Portfolio.from_signals(...)`
- `build_ranked_metrics`: PnL/trade/risk metrics table
- `plot_top_n`: equity, drawdown, trades for top configs

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

### Build backtest features from logs
```bash
python backtest/build_backtest_inputs.py \
  --log_dir ./logs \
  --out_csv ./backtest/backtest_inputs.csv \
  --windows 10,30,60
```

### Run vectorized backtest sweep
```bash
python backtest/backtest.py
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
```

### 4) Install and start systemd service
```bash
sudo cp /opt/aster/deploy/gce/aster.service /etc/systemd/system/aster.service
sudo systemctl daemon-reload
sudo systemctl enable aster
sudo systemctl start aster
```

### 5) Operate and verify
```bash
sudo systemctl status aster
journalctl -u aster -f
ls -lh /opt/aster/logs
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
- `deploy/gce/fetch_secrets.sh`
- `deploy/gce/run_strategy.sh`
- `deploy/gce/aster.service`

```bash
cd /opt/aster
git pull --ff-only
sudo bash /opt/aster/deploy/gce/bootstrap.sh
sudo cp /opt/aster/deploy/gce/aster.service /etc/systemd/system/aster.service
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

## Notes

- Paths like `./logs` are relative to your command working directory.
- Keep API credentials out of version control.
- Live and backtest flows are intentionally separated:
  - `core/*` for streaming + execution
  - `backtest/*` for offline evaluation
