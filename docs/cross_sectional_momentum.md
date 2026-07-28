# Cross-Sectional Momentum Strategy

This is the candidate replacement for the current per-symbol 1-minute threshold strategy.
It is a portfolio strategy: it ranks the full universe, targets a long/short basket, and
rebalances positions toward target weights. It should not use routine TP/SL/trailing exits.

## Inputs

Minimum signal inputs:

- `timestamp`
- `symbol`
- closed 1-minute kline `close`
- `funding_rate`

Live execution and risk inputs:

- top-of-book bid/ask/mid for spread filters and IOC limit prices
- mark price for account risk, position valuation, and reporting
- exchange filters from `exchangeInfo`: tick size, step size, min/max qty, min notional
- current signed position quantity per symbol
- account equity / `totalMarginBalance`
- recent realized returns for volatility and correlation reporting

The live stack already tracks the required market feeds for the configured symbols. The
research backtest consumes the feature CSV built by `backtest/build_backtest_inputs.py`.

## Config

Candidate config lives in:

```text
core/config_cross_sectional_momentum.json
```

Key fields:

- `symbols`: exactly the portfolio universe to rank. Six symbols with `leg_count=3`
  means the strategy is always long three and short three when all signals are valid.
- `bar_minutes`: slower bar used by the signal. The current candidate is `240` minutes.
- `short_ema_windows` / `long_ema_windows`: EMA pairs from the Chu/Chan/Zhang method.
- `short_vol_window` / `long_signal_vol_window`: two-step signal normalization windows.
- `target_gross_exposure`: total absolute portfolio exposure as a fraction of equity.
- `max_symbol_weight`: absolute cap per symbol.
- `max_spread_bps` / `max_funding_abs_bps`: per-symbol eligibility filters.
- `routine_daily_flatten`: should remain `false` for this strategy.
- `use_bracket_exits`: should remain `false` for this strategy.
- `dry_run_equity_usd`: account equity used for dry-run target notional estimates.

With `dry_run_equity_usd=850`, `target_gross_exposure=1.0`, and `leg_count=3`,
the target book is roughly:

- gross notional: `$850`
- long notional: `$425`
- short notional: `$425`
- per-symbol absolute notional: `$141.67`

This is market exposure, not margin consumed. With isolated leverage set to `3`,
initial margin would be roughly `$850 / 3 = $283.33`, leaving the rest of the
account as buffer before fees, funding, maintenance margin, and adverse moves.

## Research Commands

Six-symbol 4h run:

```bash
python3 backtest/cross_sectional_momentum.py \
  --inputs_csv backtest/backtest_inputs.csv \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,AAVEUSDT \
  --bar 4h \
  --short_vol_window 6 \
  --long_signal_vol_window 42 \
  --leg_count 3 \
  --run_name cs_mom_4h_six_assets
```

## Live/Dry-Run Command

Run the portfolio strategy in dry-run mode from the `core` directory:

```bash
cd core
python portfolio_main.py \
  --config_file ./config_cross_sectional_momentum.json \
  --poll_time 21600 \
  --log_dir ../logs
```

Live orders require both:

- `"enable_trading": true` in `core/config_cross_sectional_momentum.json`
- `--enable_trading true` on the command line

That double opt-in is intentional.

If one symbol is missing or unreliable, use five symbols with two legs:

```bash
python3 backtest/cross_sectional_momentum.py \
  --inputs_csv backtest/backtest_inputs.csv \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT \
  --bar 4h \
  --short_vol_window 6 \
  --long_signal_vol_window 42 \
  --leg_count 2 \
  --run_name cs_mom_4h_five_assets
```

## Runtime Mismatches To Fix Before Trading

- `core/main.py` currently instantiates one independent strategy per symbol.
- `core/runtime/loop.py` currently treats signals as enter-if-flat events.
- Existing TP/SL/trailing exits would break basket exposure and should be disabled.
- Existing `entry_halt_utc` and `force_exit_utc` would block or flatten the basket.
- Existing per-symbol cooldown is a poor fit; use turnover and rebalance thresholds.
- Reporting needs portfolio snapshots and daily marked returns, not exit-only lifecycle rows.

## Questions To Resolve

- Confirm HYPEUSDT has enough clean tracked history, live liquidity, and acceptable spread.
- Confirm `target_gross_exposure=1.0` is the desired first live exposure.
- Add account/position daily snapshots before relying on the daily report for this strategy.
