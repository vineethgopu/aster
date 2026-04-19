from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Dict


TABLE_SQL: Dict[str, str] = {
    "kline": """
        SELECT ts_unix_ms, ts_dt_utc, symbol, k1_start_ms, k1_close_ms, k1_open, k1_high, k1_low, k1_close,
               k1_base_vol, k1_quote_vol, k1_trades, k1_closed, date, hour, minute, second
        FROM `{project}.{dataset}.kline`
        WHERE date = DATE("{date}")
          AND symbol = "{symbol}"
          AND ts_unix_ms BETWEEN {start_ms} AND {end_ms}
        ORDER BY ts_unix_ms
    """,
    "book_ticker": """
        SELECT ts_unix_ms, ts_dt_utc, symbol, bid_px, bid_qty, ask_px, ask_qty, spread, mid, imbalance, weighted_mid,
               date, hour, minute, second
        FROM `{project}.{dataset}.book_ticker`
        WHERE date = DATE("{date}")
          AND symbol = "{symbol}"
          AND ts_unix_ms BETWEEN {start_ms} AND {end_ms}
        ORDER BY ts_unix_ms
    """,
    "mark_price": """
        SELECT ts_unix_ms, ts_dt_utc, symbol, mark_px, index_px, funding_rate, next_funding_time_ms, mark_index_bps,
               date, hour, minute, second
        FROM `{project}.{dataset}.mark_price`
        WHERE date = DATE("{date}")
          AND symbol = "{symbol}"
          AND ts_unix_ms BETWEEN {start_ms} AND {end_ms}
        ORDER BY ts_unix_ms
    """,
    "agg_trade_1s": """
        SELECT ts_unix_ms, ts_dt_utc, symbol, n_trades_1s, sum_qty_1s, vwap_1s, buy_qty_1s, sell_qty_1s,
               buy_notional_1s, sell_notional_1s, date, hour, minute, second
        FROM `{project}.{dataset}.agg_trade_1s`
        WHERE date = DATE("{date}")
          AND symbol = "{symbol}"
          AND ts_unix_ms BETWEEN {start_ms} AND {end_ms}
        ORDER BY ts_unix_ms
    """,
    "depth5": """
        SELECT ts_unix_ms, ts_dt_utc, symbol, bid1_px, bid2_px, bid3_px, bid4_px, bid5_px,
               bid1_qty, bid2_qty, bid3_qty, bid4_qty, bid5_qty,
               ask1_px, ask2_px, ask3_px, ask4_px, ask5_px,
               ask1_qty, ask2_qty, ask3_qty, ask4_qty, ask5_qty,
               obi5, date, hour, minute, second
        FROM `{project}.{dataset}.depth5`
        WHERE date = DATE("{date}")
          AND symbol = "{symbol}"
          AND ts_unix_ms BETWEEN {start_ms} AND {end_ms}
        ORDER BY ts_unix_ms
    """,
}

MAX_BQ_ROWS = 1_000_000


def _run_bq_csv(sql: str) -> str:
    proc = subprocess.run(
        [
            "bq",
            "query",
            "--use_legacy_sql=false",
            "--format=csv",
            f"--max_rows={MAX_BQ_ROWS}",
            sql,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "bq query failed")
    return proc.stdout


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case-dir", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="aster")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--date", required=False, default="2026-04-16")
    args = parser.parse_args()

    case_dir = Path(args.case_dir)
    inputs_dir = case_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    summary: Dict[str, int] = {}
    for table_name, template in TABLE_SQL.items():
        sql = " ".join(template.split()).format(
            project=args.project,
            dataset=args.dataset,
            symbol=args.symbol,
            start_ms=args.start_ms,
            end_ms=args.end_ms,
            date=args.date,
        )
        csv_text = _run_bq_csv(sql)
        out_path = inputs_dir / f"{table_name}.csv"
        out_path.write_text(csv_text, encoding="utf-8")
        summary[table_name] = max(0, len(csv_text.splitlines()) - 1)
        print(f"[FETCHED] {table_name} rows={summary[table_name]} path={out_path}")

    metadata = {
        "symbol": args.symbol,
        "project": args.project,
        "dataset": args.dataset,
        "date": args.date,
        "start_ms": args.start_ms,
        "end_ms": args.end_ms,
        "row_counts": summary,
    }
    (case_dir / "fetch_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"[DONE] wrote {case_dir / 'fetch_metadata.json'}")


if __name__ == "__main__":
    main()
