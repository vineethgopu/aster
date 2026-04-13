# Aster Repo Instructions

This file applies to the entire repository rooted at `~/crypto/aster`.

## Primary Repo

- This is the main codebase to understand and edit for the Aster live trading stack, backtests, and GCE operations.
- The live system is a directional mid-frequency strategy on Aster USDT perpetual futures.
- Current production universe is `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`, and `BNBUSDT`.
- Owner context says the intended production profile is roughly 10-30 trades per day with order lifetimes around 30-60 minutes. Treat that as strategy intent; verify against logs or backtests before stating it as measured fact.

## Working Agreements

- Ask before editing files, even for small code changes, unless the user explicitly requested those edits in the current turn.
- Ask before any `git commit`, `git push`, branch rewrite, destructive git action, package install, or `sudo` command.
- Read-only repo inspection is pre-approved. Typical commands include `rg`, `grep`, `find`, `ls`, `awk`, `sed`, `cat`, `less`, `wc`, `git status`, `git log`, `git diff`, `python -c`, and `node -e`.
- Running existing repo scripts for inspection or validation is fine when they do not install packages, require elevated privileges, or mutate deployment state.
- The worktree may be dirty. Never revert user changes. Check `git status` before editing.

## Architecture Map

- `core/main.py`: live orchestration loop, daily risk blockers, entry/exit lifecycle logging, cooldowns, and runtime wiring.
- `core/strategy.py`: pure signal logic from closed 1-minute bars plus spread, funding, and opening-loss blockers.
- `core/order.py`: exchange filters, taker IOC entry, reduce-only exits, fill reconciliation, and margin kill logic.
- `core/client.py`: REST bootstrap, public WS caches, private user stream, REST fallback reconciliation, and per-symbol snapshots.
- `core/config_current.json`: per-symbol production parameters.
- `backtest/`: BigQuery feature builder plus VectorBT parameter sweep.
- `deploy/gce/`: VM bootstrap, secrets, timers/services, BigQuery loads, cleanup, and email reporting.

## Strategy Notes

- Entries are directional, not market-making.
- Signal evaluation happens on each newly closed 1-minute bar inside a 1-second runtime loop.
- Signal logic uses close-to-close return versus a Rogers-Satchell volatility threshold plus a 1-minute volume regime filter.
- Entry blockers use spread, funding, and opening loss versus mark/touch.
- Execution is taker-first: entry is an IOC LIMIT at touch, and exits are armed immediately with reduce-only TP/SL/trailing triggers.
- Runtime prefers private user-stream account truth and uses REST fallback for recovery and stale-stream handling.
- Daily safeguards include a drawdown blocker, end-of-day entry halt, hard force-exit window, and re-entry cooldown.

## Repo-local Context Pack

- Read the relevant repo-local skill before changing that area:
  - `.codex/skills/aster-strategy/SKILL.md`
  - `.codex/skills/aster-market-data/SKILL.md`
  - `.codex/skills/aster-order-lifecycle/SKILL.md`
  - `.codex/skills/aster-gcp-ops/SKILL.md`
- If a task spans multiple areas, read all applicable skill files.
- For performance claims or strategy-behavior claims, prefer logs, backtest outputs, and current config over assumptions.
