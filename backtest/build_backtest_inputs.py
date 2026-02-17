from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

import numpy as np
import pandas as pd


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


def _read_csv(path: Path, required_cols: Iterable[str]) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")
    return df


def _prepare_kline(kline: pd.DataFrame) -> pd.DataFrame:
    kline = kline.copy()
    num_cols = [
        "ts_unix_ms",
        "k1_start_ms",
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

    # Backtest mode: keep the last snapshot seen in each minute for each symbol,
    # even when k1_closed is False, to avoid dropping incomplete candle minutes.
    kline["minute_bucket_ms"] = (kline["ts_unix_ms"] // 60000) * 60000
    kline = kline.sort_values(["symbol", "minute_bucket_ms", "ts_unix_ms"])
    kline = kline.drop_duplicates(subset=["symbol", "minute_bucket_ms"], keep="last")

    # Prefer exchange-provided close time when present, else minute end.
    minute_end_ms = kline["minute_bucket_ms"] + 60000 - 1
    kline["bar_ts_ms"] = np.where(kline["k1_close_ms"].notna(), kline["k1_close_ms"], minute_end_ms).astype("int64")
    kline["timestamp"] = pd.to_datetime(kline["bar_ts_ms"], unit="ms", utc=True)
    return kline


def _prepare_book(book: pd.DataFrame) -> pd.DataFrame:
    book = book.copy()
    for c in ["ts_unix_ms", "bid_px", "ask_px", "spread", "mid"]:
        book[c] = pd.to_numeric(book[c], errors="coerce")
    book = book.dropna(subset=["symbol", "ts_unix_ms"]).sort_values(["symbol", "ts_unix_ms"])
    return book


def _prepare_mark(mark: pd.DataFrame) -> pd.DataFrame:
    mark = mark.copy()
    for c in ["ts_unix_ms", "mark_px", "funding_rate"]:
        mark[c] = pd.to_numeric(mark[c], errors="coerce")
    mark = mark.dropna(subset=["symbol", "ts_unix_ms"]).sort_values(["symbol", "ts_unix_ms"])
    return mark


def _merge_symbol(sym: str, kline_sym: pd.DataFrame, book_sym: pd.DataFrame, mark_sym: pd.DataFrame) -> pd.DataFrame:
    base = kline_sym.sort_values("bar_ts_ms").copy()
    if base.empty:
        return base

    # Align nearest latest BBO/funding snapshot at or before bar close.
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


def build_features(log_dir: Path, windows: List[int]) -> pd.DataFrame:
    kline = _read_csv(
        log_dir / "kline.csv",
        ["symbol", "ts_unix_ms", "k1_close_ms", "k1_open", "k1_high", "k1_low", "k1_close", "k1_base_vol", "k1_closed"],
    )
    book = _read_csv(log_dir / "bookTicker.csv", ["symbol", "ts_unix_ms", "bid_px", "ask_px"])
    mark = _read_csv(log_dir / "markPrice.csv", ["symbol", "ts_unix_ms", "mark_px", "funding_rate"])

    kline = _prepare_kline(kline)
    book = _prepare_book(book)
    mark = _prepare_mark(mark)

    symbols = sorted(set(kline["symbol"].dropna().astype(str)))
    out_parts: List[pd.DataFrame] = []
    for sym in symbols:
        ks = kline[kline["symbol"] == sym].copy()
        bs = book[book["symbol"] == sym].copy()
        ms = mark[mark["symbol"] == sym].copy()

        merged = _merge_symbol(sym, ks, bs, ms)
        if merged.empty:
            continue

        # Raw series used by backtest
        merged = merged.sort_values("timestamp")
        merged["close"] = merged["k1_close"].astype(float)
        merged["vol1m"] = merged["k1_base_vol"].astype(float)
        merged["ret_bps"] = merged["close"].pct_change() * 1e4

        # Spread / funding / opening loss proxies
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

        # RS volatility and rolling volume features
        rs_var = _rs_var(merged["k1_open"], merged["k1_high"], merged["k1_low"], merged["k1_close"])
        merged["rs_var_1m"] = rs_var
        for w in windows:
            merged[f"rs_vol_{w}m_bps"] = _rolling_rs_vol_bps(rs_var, window=w)
            merged[f"avg_vol_{w}m"] = merged["vol1m"].rolling(w, min_periods=w).mean()

        out_parts.append(merged)

    if not out_parts:
        raise ValueError("No feature rows were produced. Check logs and symbols.")

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_dir", type=str, default="./logs")
    parser.add_argument("--out_csv", type=str, default="./backtest/backtest_inputs.csv")
    parser.add_argument("--out_parquet", type=str, default="")
    parser.add_argument("--windows", type=str, default="10,30,60")
    args = parser.parse_args()

    windows = [int(x.strip()) for x in args.windows.split(",") if x.strip()]
    features = build_features(Path(args.log_dir), windows=windows)
    features.to_csv(args.out_csv, index=False)
    print(f"Wrote {len(features)} rows to {args.out_csv}")
    if args.out_parquet:
        features.to_parquet(args.out_parquet, index=False)
        print(f"Wrote {len(features)} rows to {args.out_parquet}")


if __name__ == "__main__":
    main()
