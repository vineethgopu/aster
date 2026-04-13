---
name: aster-market-data
description: Use when working on Aster REST or WebSocket market-data or account-data logic, startup snapshots, cache design, BigQuery log tables, or understanding which feeds are tracked and why.
---

# Aster Market Data and API

## When to use

- Changes in `core/client.py`
- Questions about REST bootstrap, WS subscriptions, cache contents, or user-stream health
- Questions about what market data is logged, uploaded, or used by the strategy

## Public REST used

- `klines(symbol, "1m", limit=20)` seeds recent 1-minute bars
- `book_ticker(symbol)` seeds touch prices and is also used at order submit time
- `mark_price(symbol)` seeds mark, index, funding, and next funding time
- `agg_trades(symbol, limit=100)` seeds recent prints
- `depth(symbol, limit=5)` seeds top-5 depth
- `exchange_info()` supplies tick size, step size, min qty, max qty, and min notional filters

## Private/account REST used

- `new_listen_key()`, `renew_listen_key()`, `close_listen_key()` manage the user stream
- `get_position_risk()` is the signed position fallback and REST reconciliation source
- `account()` supplies `totalMarginBalance` and `totalMaintMargin`
- `core/order.py` also uses:
  - `new_order(...)`
  - signed `GET /fapi/v1/order`
  - signed `DELETE /fapi/v1/order`
  - `get_account_trades(...)`
  - `change_leverage(...)`
  - `change_margin_type(...)`

## WebSocket streams

- Public combined streams in `core/client.py`:
  - `@kline_1m`
  - `@bookTicker`
  - `@markPrice@1s`
  - `@aggTrade`
  - `@depth5@100ms`
- Private user-data stream tracks:
  - `ACCOUNT_UPDATE`
  - `ORDER_TRADE_UPDATE`
  - `listenKeyExpired`

## Why each feed matters

- 1-minute kline:
  - closed-bar return
  - Rogers-Satchell variance
  - rolling volume warm-up and signal trigger
- bookTicker:
  - spread gate
  - mid and weighted-mid diagnostics
  - opening-loss blocker
  - taker IOC entry price
- markPrice and funding:
  - funding blocker
  - mark-based stop context
  - mark tracking for trade lifecycle rows
  - daily equity and risk context
- aggTrade:
  - 1-second trade-flow logs
  - order-lifetime market-activity stats
  - future execution analytics and research
- depth5:
  - top-of-book depth diagnostics
  - order book imbalance over 5 levels
  - future microstructure features
- user stream:
  - low-latency order state
  - position truth
  - balance changes
- REST fallback:
  - startup seeding
  - stale user stream protection
  - account/order recovery

## Snapshot shape

- `client.get_symbol_rows()` returns, per symbol:
  - `bars`
  - `bbo`
  - `funding`
  - `trades_1s`
  - `l2`
  - `account`, `order_updates_1s`, and `user_stream` when trading is enabled

## Logging and BigQuery

- `core/logs.py` writes dated CSVs:
  - `kline`
  - `bookTicker`
  - `markPrice`
  - `aggTrade_1s`
  - `depth5`
- `orders_YYYYMMDD.csv` is written from `core/main.py` when a trade is finalized
- Daily loader: `deploy/gce/bq_load_logs.py`
- Current backtest feature builder reads only:
  - `kline`
  - `book_ticker`
  - `mark_price`
- Depth and agg-trade data are primarily for live analytics, diagnostics, and future research, not the current backtest feature set

## Change guidance

- Keep cache keys and CSV column names stable unless you also update loaders, BigQuery tables, and downstream queries.
- If user-stream health logic changes, preserve REST fallback and observability.
- If you add a new tracked feed, decide whether it belongs only in live logic, in logs, or also in BigQuery/backtests.
