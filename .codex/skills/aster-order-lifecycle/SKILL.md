---
name: aster-order-lifecycle
description: Use when changing entry or exit execution, order sizing, fill confirmation, position recovery, taker versus maker behavior, or TP/SL/trailing-stop handling in the Aster strategy.
---

# Aster Order Lifecycle

## When to use

- Changes in `core/execution/order.py`, `core/runtime/loop.py`, or `core/runtime/lifecycle.py`
- Questions about taker and maker behavior, position state, or close logic
- Debugging fills, trigger handling, lifecycle rows, or order recovery

## Current execution model

- Entries are taker-only.
- Entry order type is IOC LIMIT that crosses touch:
  - BUY at ask
  - SELL at bid
- Maker entry is not active. `maker_order_id` is effectively always `None`.
- Touch price is REST-first via `book_ticker()`, with cache fallback.

## Sizing and exchange filters

- Exchange filters are loaded from `exchange_info()` and cached.
- Quantity and price are normalized against:
  - `PRICE_FILTER.tickSize`
  - `LOT_SIZE.stepSize`, `minQty`, `maxQty`
  - `MIN_NOTIONAL` or `NOTIONAL`
- `compute_qty_for_notional()` rounds up enough to satisfy the requested USD notional and exchange minimums.

## Fill confirmation and recovery

- `_confirm_order_fill()` polls order status briefly and then reconciles against `get_account_trades()`.
- An entry is not considered dead just because the first order query looks empty.
- `core/runtime/loop.py` keeps `pending_entries` so a late user-stream fill or recovered exchange position can still arm exits.
- Exit detection prefers private WS order updates; REST polling is a fallback path.

## Exit model

- After a confirmed or recovered fill, three reduce-only exits are armed:
  - `TAKE_PROFIT_MARKET` with `workingType=CONTRACT_PRICE`
  - `STOP_MARKET` with `workingType=MARK_PRICE`
  - `TRAILING_STOP_MARKET` with activation price and callback rate
- Standard trigger payload assumptions:
  - `reduceOnly=True`
  - `priceProtect=TRUE`
- When the position is flat, sibling exits are cancelled.

## Forced exits and guards

- `close_position()` refreshes quantity and side from exchange truth when possible before submitting the close.
- Forced-flat paths include:
  - daily drawdown blocker
  - daily cutoff
  - margin kill via `totalMarginBalance / totalMaintMargin`
  - manual or operator close
- Forced closes default to MARKET.

## Position truth

- Prefer private user-stream updates from `core/exchange/client.py`.
- REST is still needed for:
  - startup seeding
  - stale-stream recovery
  - late order reconciliation
  - pre-close position verification

## Lifecycle logging

- Completed trades are written to `orders_YYYYMMDD.csv`.
- Lifecycle rows include:
  - entry and exit IDs
  - send and fill times
  - prices and quantity
  - fees
  - duration
  - lifetime market volume and OHLC
  - mark-price change
  - gross and total PnL
- Trade-alert emails reuse this finalized lifecycle view.

## Change guidance

- Do not break `reduceOnly`, trigger working types, or rounding behavior without a clear exchange-specific reason.
- If exit semantics change, also inspect `core/runtime/loop.py`, `core/runtime/lifecycle.py`, `README.md`, and backtest assumptions.
- Avoid adding maker logic casually; the current runtime, recovery logic, and analytics assume aggressive entry.
