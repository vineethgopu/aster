---
name: aster-strategy
description: Use when working on the Aster directional mid-frequency strategy, perp futures domain assumptions, signal logic, per-symbol runtime parameters, or why the live system enters or blocks trades for BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, or BNBUSDT.
---

# Aster Strategy

## When to use

- Questions about the underlying strategy or perpetual futures context.
- Changes to signal logic, blockers, daily risk controls, sizing, or production params.
- Explaining why a symbol entered, failed to enter, or was force-closed.

## Mental model

- This is a directional mid-frequency perp strategy, not a market-making strategy.
- The live loop runs once per second, but new entry signals only appear when a fresh closed 1-minute bar arrives.
- The intended production universe is `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`, and `BNBUSDT`.
- Owner context says the target operating profile is roughly 10-30 trades per day with many positions lasting 30-60 minutes.

## Entry logic

- `core/signals/strategy.py` evaluates each symbol only on a newly closed 1-minute bar.
- Indicator 1 is directional momentum scaled by realized volatility:
  - long if `ret_bps > k * RS_vol(T)`
  - short if `ret_bps < -k * RS_vol(T)`
- Indicator 2 is a volume regime filter:
  - current 1-minute base volume must exceed `n * rolling_avg_volume(V)`
- Warm-up matters:
  - no entries until both rolling windows are fully populated
- Blockers must all pass:
  - spread inside `max_spread_ticks * tickSize` when exchange tick size is known, otherwise inside raw `max_spread`
  - absolute funding in bps must be <= `funding_max`
  - opening loss versus mark/touch must be <= `min(10, max(5, 2 * spread_bps))`

## Runtime controls

- Per-symbol production parameters live in `core/config_current.json`.
- Global defaults are exposed through the live entrypoint `core/main.py`, loaded by `core/runtime/config.py`, and mirrored in `deploy/gce/env.sample`.
- If `--order_notional` is unset, `core/runtime/loop.py` computes:
  - `start_of_day_balance * (risk_pct / 100) * target_leverage`
- New entries are blocked by:
  - daily drawdown blocker
  - `entry_halt_utc`
  - symbol cooldown
  - existing exchange position
  - missing balance or missing order notional

## Exit shaping

- `core/runtime/loop.py` adjusts exit levels before submit.
- The breakeven floor includes:
  - `2 * taker_fee_bps`
  - estimated opening loss
  - `abs(funding_bps) / 8`
- Trailing activation is floored above breakeven using `activation_buffer_bps`.
- Take profit is floored above activation by `min_tp_gap_bps`.
- Stop loss remains the symbol-level configured value.

## Important files

- `README.md`
- `core/signals/strategy.py`
- `core/main.py`
- `core/runtime/config.py`
- `core/runtime/loop.py`
- `core/config_current.json`
- `backtest/config_grid.json`
- `backtest/backtest.py`

## Change guidance

- Keep live logic and backtest assumptions aligned. If signal or exit semantics change, check whether `backtest/backtest.py` or `backtest/build_backtest_inputs.py` also needs updates.
- Treat symbol-specific params as production state, not generic defaults.
- Do not describe measured performance without checking logs or backtest outputs.
