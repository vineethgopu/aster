# Validation Suite

This folder holds reusable historical replay cases for the live strategy.

## Goals

- freeze a market-data window from BigQuery into local CSV fixtures
- keep the candidate config snapshots used for the replay
- replay the real `Strategy` logic over those fixtures
- simulate a single-position lifecycle with cooldowns so the output stays closer to live runtime behavior
- estimate the corresponding entry/exit path even when the original `orders` row is missing

## Layout

- `fetch_case_data.py`: materialize market-data fixtures from BigQuery into a case folder
- `replay_strategy_case.py`: run signal replay and simple exit inference over saved fixtures
- `cases/<case_id>/case_manifest.json`: case metadata and assumptions
- `cases/<case_id>/configs/`: candidate config snapshots
- `cases/<case_id>/inputs/`: frozen CSVs from BigQuery

## Current Case

- `xrp_20260416`
  - symbol: `XRPUSDT`
  - fixture window: `2026-04-16 13:30:00 UTC` through `2026-04-16 20:00:00 UTC`
  - original `orders` row is unavailable, so replay focuses on signal reproduction and inferred exit behavior

## Typical Flow

Fetch fixtures:

```bash
python3 validation/fetch_case_data.py \
  --case-dir validation/cases/xrp_20260416 \
  --project project-c2ebd65c-60ef-4434-b2f \
  --dataset aster \
  --symbol XRPUSDT \
  --start-ms 1776346200000 \
  --end-ms 1776369600000
```

Replay with a candidate config:

```bash
python3 validation/replay_strategy_case.py \
  --case-dir validation/cases/xrp_20260416 \
  --config validation/cases/xrp_20260416/configs/config_candidate_user_corrected.json \
  --symbol XRPUSDT
```

## Notes

- The replay script uses the real `Strategy` implementation from `core/signals/strategy.py`.
- Exit inference is approximate because the exact historical private-order lifecycle is missing.
- The replay currently approximates exit triggers from saved market data; it does not replay private WebSocket fills.
- The point of this suite is repeatability across future runtime/lifecycle refactors, not a perfect exchange-faithful backtest.
