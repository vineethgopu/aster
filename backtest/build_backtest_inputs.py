from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery


KLINE_TABLE = "kline"
BOOK_TABLE = "book_ticker"
MARK_TABLE = "mark_price"


def _bps_ret(px: pd.Series, ref: pd.Series) -> pd.Series:
    return 1e4 * (px / ref - 1.0)


def _rs_var(o: pd.Series, h: pd.Series, l: pd.Series, c: pd.Series) -> pd.Series:
    valid = (o > 0) & (h > 0) & (l > 0) & (c > 0)
    out = pd.Series(np.nan, index=o.index, dtype=float)
    oo = np.log(h[valid] / o[valid]) * np.log(h[valid] / c[valid])
    ll = np.log(l[valid] / o[valid]) * np.log(l[valid] / c[valid])
    out.loc[valid] = oo + ll
    return out


def _rolling_rs_vol_bps(rs_var: pd.Series, window: int) -> pd.Series:
    return 1e4 * np.sqrt(rs_var.rolling(window, min_periods=window).mean())


def _parse_date(s: str) -> Optional[date]:
    s = str(s or "").strip()
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_where_and_params(
    symbols: Sequence[str],
    start_date: Optional[date],
    end_date: Optional[date],
) -> Tuple[str, List[bigquery.query.ArrayQueryParameter | bigquery.query.ScalarQueryParameter]]:
    clauses: List[str] = []
    params: List[bigquery.query.ArrayQueryParameter | bigquery.query.ScalarQueryParameter] = []

    if symbols:
        clauses.append("symbol IN UNNEST(@symbols)")
        params.append(bigquery.ArrayQueryParameter("symbols", "STRING", list(symbols)))
    if start_date is not None:
        clauses.append("date >= @start_date")
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date is not None:
        clauses.append("date <= @end_date")
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))

    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def _query_table(
    client: bigquery.Client,
    table_fqn: str,
    columns: Sequence[str],
    symbols: Sequence[str],
    start_date: Optional[date],
    end_date: Optional[date],
) -> pd.DataFrame:
    where_sql, params = _build_where_and_params(symbols=symbols, start_date=start_date, end_date=end_date)
    sql = f"""
        SELECT {", ".join(columns)}
        FROM `{table_fqn}`
        {where_sql}
        ORDER BY symbol, ts_unix_ms
    """
    cfg = bigquery.QueryJobConfig(query_parameters=params)
    job = client.query(sql, job_config=cfg)
    result = job.result()
    cols = [field.name for field in result.schema]
    rows = [dict(row.items()) for row in result]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows, columns=cols)


def _load_inputs_bigquery(
    client: bigquery.Client,
    project: str,
    dataset: str,
    symbols: Sequence[str],
    start_date: Optional[date],
    end_date: Optional[date],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    kline_cols = [
        "symbol",
        "ts_unix_ms",
        "k1_close_ms",
        "k1_open",
        "k1_high",
        "k1_low",
        "k1_close",
        "k1_base_vol",
        "k1_closed",
    ]
    book_cols = ["symbol", "ts_unix_ms", "bid_px", "ask_px", "spread", "mid"]
    mark_cols = ["symbol", "ts_unix_ms", "mark_px", "funding_rate"]

    kline = _query_table(
        client=client,
        table_fqn=f"{project}.{dataset}.{KLINE_TABLE}",
        columns=kline_cols,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    book = _query_table(
        client=client,
        table_fqn=f"{project}.{dataset}.{BOOK_TABLE}",
        columns=book_cols,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    mark = _query_table(
        client=client,
        table_fqn=f"{project}.{dataset}.{MARK_TABLE}",
        columns=mark_cols,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    return kline, book, mark


def _prepare_kline(kline: pd.DataFrame) -> pd.DataFrame:
    kline = kline.copy()
    num_cols = [
        "ts_unix_ms",
        "k1_open",
        "k1_high",
        "k1_low",
        "k1_close",
        "k1_base_vol",
        "k1_close_ms",
    ]
    for c in num_cols:
        kline[c] = pd.to_numeric(kline[c], errors="coerce")
    kline["k1_closed"] = kline["k1_closed"].astype(str).str.lower().isin(["true", "1"])
    kline = kline.dropna(subset=["symbol", "ts_unix_ms", "k1_open", "k1_high", "k1_low", "k1_close", "k1_base_vol"])

    # Keep the latest snapshot per minute for backtest completeness.
    kline["minute_bucket_ms"] = (kline["ts_unix_ms"] // 60000) * 60000
    kline = kline.sort_values(["symbol", "minute_bucket_ms", "ts_unix_ms"])
    kline = kline.drop_duplicates(subset=["symbol", "minute_bucket_ms"], keep="last")

    minute_end_ms = kline["minute_bucket_ms"] + 60000 - 1
    kline["bar_ts_ms"] = np.where(kline["k1_close_ms"].notna(), kline["k1_close_ms"], minute_end_ms).astype("int64")
    kline["timestamp"] = pd.to_datetime(kline["bar_ts_ms"], unit="ms", utc=True)
    return kline


def _prepare_book(book: pd.DataFrame) -> pd.DataFrame:
    book = book.copy()
    for c in ["ts_unix_ms", "bid_px", "ask_px", "spread", "mid"]:
        book[c] = pd.to_numeric(book[c], errors="coerce")
    return book.dropna(subset=["symbol", "ts_unix_ms"]).sort_values(["symbol", "ts_unix_ms"])


def _prepare_mark(mark: pd.DataFrame) -> pd.DataFrame:
    mark = mark.copy()
    for c in ["ts_unix_ms", "mark_px", "funding_rate"]:
        mark[c] = pd.to_numeric(mark[c], errors="coerce")
    return mark.dropna(subset=["symbol", "ts_unix_ms"]).sort_values(["symbol", "ts_unix_ms"])


def _merge_symbol(sym: str, kline_sym: pd.DataFrame, book_sym: pd.DataFrame, mark_sym: pd.DataFrame) -> pd.DataFrame:
    base = kline_sym.sort_values("bar_ts_ms").copy()
    if base.empty:
        return base

    if not book_sym.empty:
        base = pd.merge_asof(
            base,
            book_sym[["ts_unix_ms", "bid_px", "ask_px", "spread", "mid"]].sort_values("ts_unix_ms"),
            left_on="bar_ts_ms",
            right_on="ts_unix_ms",
            direction="backward",
        )
    else:
        base["bid_px"] = np.nan
        base["ask_px"] = np.nan
        base["spread"] = np.nan
        base["mid"] = np.nan

    if not mark_sym.empty:
        base = pd.merge_asof(
            base,
            mark_sym[["ts_unix_ms", "mark_px", "funding_rate"]].sort_values("ts_unix_ms"),
            left_on="bar_ts_ms",
            right_on="ts_unix_ms",
            direction="backward",
            suffixes=("", "_mark"),
        )
    else:
        base["mark_px"] = np.nan
        base["funding_rate"] = np.nan

    base["symbol"] = sym
    return base


def build_features(
    client: bigquery.Client,
    project: str,
    dataset: str,
    windows: List[int],
    symbols: Sequence[str],
    start_date: Optional[date],
    end_date: Optional[date],
) -> pd.DataFrame:
    kline, book, mark = _load_inputs_bigquery(
        client=client,
        project=project,
        dataset=dataset,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )

    kline = _prepare_kline(kline)
    book = _prepare_book(book)
    mark = _prepare_mark(mark)

    symbol_list = sorted(set(kline["symbol"].dropna().astype(str)))
    out_parts: List[pd.DataFrame] = []
    for sym in symbol_list:
        ks = kline[kline["symbol"] == sym].copy()
        bs = book[book["symbol"] == sym].copy()
        ms = mark[mark["symbol"] == sym].copy()

        merged = _merge_symbol(sym, ks, bs, ms)
        if merged.empty:
            continue

        merged = merged.sort_values("timestamp")
        merged["close"] = merged["k1_close"].astype(float)
        merged["vol1m"] = merged["k1_base_vol"].astype(float)
        merged["ret_bps"] = merged["close"].pct_change() * 1e4

        merged["mid"] = merged["mid"].where(merged["mid"] > 0, 0.5 * (merged["bid_px"] + merged["ask_px"]))
        merged["spread_bps"] = np.where(
            (merged["mid"] > 0) & merged["bid_px"].notna() & merged["ask_px"].notna(),
            1e4 * (merged["ask_px"] - merged["bid_px"]) / merged["mid"],
            np.nan,
        )
        merged["funding_bps"] = merged["funding_rate"] * 1e4

        merged["opening_loss_buy_bps"] = _bps_ret(merged["ask_px"], merged["mark_px"])
        merged["opening_loss_sell_bps"] = _bps_ret(merged["mark_px"], merged["bid_px"])
        merged["opening_loss_bps"] = np.where(
            merged["ret_bps"] >= 0,
            merged["opening_loss_buy_bps"],
            merged["opening_loss_sell_bps"],
        )

        rs_var = _rs_var(merged["k1_open"], merged["k1_high"], merged["k1_low"], merged["k1_close"])
        merged["rs_var_1m"] = rs_var
        for w in windows:
            merged[f"rs_vol_{w}m_bps"] = _rolling_rs_vol_bps(rs_var, window=w)
            merged[f"avg_vol_{w}m"] = merged["vol1m"].rolling(w, min_periods=w).mean()

        out_parts.append(merged)

    if not out_parts:
        raise ValueError("No feature rows were produced. Check BigQuery data/date filters/symbols.")

    out = pd.concat(out_parts, ignore_index=True)
    out = out.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    keep_cols = [
        "timestamp",
        "symbol",
        "bar_ts_ms",
        "close",
        "vol1m",
        "ret_bps",
        "bid_px",
        "ask_px",
        "mid",
        "spread_bps",
        "mark_px",
        "funding_rate",
        "funding_bps",
        "opening_loss_buy_bps",
        "opening_loss_sell_bps",
        "opening_loss_bps",
        "rs_var_1m",
    ]
    for w in windows:
        keep_cols.append(f"rs_vol_{w}m_bps")
        keep_cols.append(f"avg_vol_{w}m")
    return out[keep_cols]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build backtest feature inputs from BigQuery market-data tables.")
    parser.add_argument("--project", type=str, default=os.getenv("GOOGLE_CLOUD_PROJECT", ""))
    parser.add_argument("--dataset", type=str, default="aster")
    parser.add_argument("--location", type=str, default="")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols, e.g. ETHUSDT,BTCUSDT")
    parser.add_argument("--start_date", type=str, default="", help="Inclusive UTC date: YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, default="", help="Inclusive UTC date: YYYY-MM-DD")
    parser.add_argument("--out_csv", type=str, default="./backtest/backtest_inputs.csv")
    parser.add_argument("--out_parquet", type=str, default="")
    parser.add_argument("--windows", type=str, default="10,30,60")
    args = parser.parse_args()

    windows = [int(x.strip()) for x in args.windows.split(",") if x.strip()]
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    client = bigquery.Client(project=(args.project or None), location=(args.location or None))
    project = args.project.strip() or client.project
    if not project:
        raise ValueError("Could not determine GCP project. Pass --project or set GOOGLE_CLOUD_PROJECT.")

    features = build_features(
        client=client,
        project=project,
        dataset=args.dataset.strip(),
        windows=windows,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    features.to_csv(out_csv, index=False)
    print(f"Wrote {len(features)} rows to {out_csv}")
    if args.out_parquet:
        out_parquet = Path(args.out_parquet)
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        features.to_parquet(out_parquet, index=False)
        print(f"Wrote {len(features)} rows to {out_parquet}")


if __name__ == "__main__":
    main()
