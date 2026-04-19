---
name: aster-validation
description: Use when validating a historical trade window, freezing BigQuery market-data fixtures, replaying candidate configs, checking whether a refactor preserved signal behavior, or interpreting replay output and its limits.
---

# Aster Validation

## When to use

- Questions about `validation/`
- Reproducing a historical entry or exit from archived market data
- Comparing candidate `core/config_current.json` snapshots against the same window
- Checking whether a refactor changed strategy behavior
- Explaining what a replay result does and does not prove

## Validation goal

- Freeze a historical market-data window into local fixtures
- Replay the real `core/signals/strategy.py` logic over that window
- Simulate a live-like single-position lifecycle with cooldowns
- Compare candidate runtime configs without claiming exchange-perfect fidelity

## Current validation assets

- `validation/fetch_case_data.py`
  - materializes BigQuery fixtures into `validation/cases/<case_id>/inputs/`
- `validation/replay_strategy_case.py`
  - replays the real `Strategy`
  - simulates one open position at a time
  - applies post-exit cooldown
  - approximates TP/SL/trailing exits from saved market data
- `validation/cases/<case_id>/case_manifest.json`
  - records the window, assumptions, and candidate configs

## Recommended workflow

1. Confirm the case window and symbol.
2. Check whether the original `orders` row exists.
3. If not, state clearly that entry/exit timing can only be approximated from market data and candidate configs.
4. Freeze the market-data inputs with `validation/fetch_case_data.py`.
5. Save the candidate config snapshots used for the replay.
6. Run `validation/replay_strategy_case.py` for each candidate config.
7. Compare:
   - whether any entry appears at all
   - entry timestamp and side
   - exit reason and approximate holding time
   - rough gross return, before fees
8. Prefer conclusions like "consistent with" or "inconsistent with" instead of claiming exact reconstruction unless private order data exists.

## Interpretation guidance

- A replay with `0` entries is strong evidence that the candidate config did not produce the trade.
- A replay with multiple entries means the config is plausible, not proven.
- Gross replay PnL is only an estimate:
  - no private fill path
  - no exact filled quantity
  - no exchange trigger latency
  - no fee model inside the replay itself
- If the replay is being used after a refactor, focus first on:
  - did the same entry appear
  - did the same side appear
  - did the lifecycle stay qualitatively similar

## Known limits

- The current replay uses market-data fixtures only.
- It does not replay private WebSocket order updates.
- It does not reconstruct IOC partial fills or trigger-engine timing exactly.
- If the BigQuery `orders` table is empty, exact lifecycle reconstruction is unavailable.

## Commands

Freeze a case:

```bash
python3 validation/fetch_case_data.py \
  --case-dir validation/cases/xrp_20260416 \
  --project project-c2ebd65c-60ef-4434-b2f \
  --dataset aster \
  --symbol XRPUSDT \
  --start-ms 1776346200000 \
  --end-ms 1776369600000 \
  --date 2026-04-16
```

Replay a candidate config:

```bash
python3 validation/replay_strategy_case.py \
  --case-dir validation/cases/xrp_20260416 \
  --config validation/cases/xrp_20260416/configs/config_candidate_user_corrected.json \
  --symbol XRPUSDT
```

## Change guidance

- Keep validation assumptions explicit in `case_manifest.json`.
- If live runtime semantics change, update the replay so it stays aligned on:
  - single-position behavior
  - cooldown behavior
  - exit-floor logic
- Do not describe replay output as production PnL without labeling it an estimate.
