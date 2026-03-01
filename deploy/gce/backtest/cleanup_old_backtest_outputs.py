#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path


BACKTEST_DATED_RE = re.compile(
    r"^backtest_(?:inputs|config|results)(?:_[A-Z0-9]+)?_(?P<yyyymmdd>\d{8})\.csv$"
)


def _utc_today_date() -> datetime.date:
    return datetime.now(timezone.utc).date()


def cleanup(results_dir: Path, retention_days: int, dry_run: bool) -> None:
    if retention_days < 1:
        raise ValueError("retention_days must be >= 1")
    if not results_dir.exists():
        print(f"[SKIP] results_dir does not exist: {results_dir}")
        return

    today = _utc_today_date()
    deleted = 0
    kept = 0
    skipped = 0

    for p in sorted(results_dir.glob("*.csv")):
        m = BACKTEST_DATED_RE.match(p.name)
        if not m:
            skipped += 1
            continue

        file_date = datetime.strptime(m.group("yyyymmdd"), "%Y%m%d").date()
        age_days = (today - file_date).days
        if age_days >= retention_days:
            if dry_run:
                print(f"[DRY_RUN] delete {p} (age_days={age_days})")
            else:
                p.unlink(missing_ok=True)
                print(f"[DELETE] {p} (age_days={age_days})")
            deleted += 1
        else:
            kept += 1

    print(
        f"[SUMMARY] retention_days={retention_days} deleted={deleted} kept={kept} "
        f"skipped_non_dated={skipped} results_dir={results_dir}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete old dated backtest CSV outputs from results dir.")
    parser.add_argument("--results_dir", type=str, default="/opt/aster/backtest/results")
    parser.add_argument("--retention_days", type=int, default=28)
    parser.add_argument("--dry_run", action="store_true")
    args = parser.parse_args()
    cleanup(results_dir=Path(args.results_dir), retention_days=args.retention_days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
