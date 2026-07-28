from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


DEFAULT_SHORT_EMA_WINDOWS = (8, 16, 32)
DEFAULT_LONG_EMA_WINDOWS = (24, 48, 96)


def _parse_int_list(raw: str) -> list[int]:
    out = [int(x.strip()) for x in str(raw or "").split(",") if x.strip()]
    if not out:
        raise ValueError("Expected at least one integer value")
    if any(x <= 0 for x in out):
        raise ValueError(f"Window values must be positive integers: {out}")
    return out


def _parse_symbols(raw: str) -> Optional[list[str]]:
    symbols = [s.strip().upper() for s in str(raw or "").split(",") if s.strip()]
    return symbols or None


def _bar_seconds(freq: str) -> float:
    seconds = pd.Timedelta(freq).total_seconds()
    if seconds <= 0:
        raise ValueError(f"Invalid bar frequency: {freq}")
    return float(seconds)


def load_price_inputs(path: Path, symbols: Optional[Iterable[str]]) -> pd.DataFrame:
    usecols = {"timestamp", "symbol", "close", "funding_rate"}
    df = pd.read_csv(path, usecols=lambda c: c in usecols)
    missing = {"timestamp", "symbol", "close"} - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "funding_rate" in df.columns:
        df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
    else:
        df["funding_rate"] = 0.0

    df = df.dropna(subset=["timestamp", "symbol", "close"])
    if symbols is not None:
        want = {s.upper() for s in symbols}
        df = df[df["symbol"].isin(want)].copy()
        missing_symbols = sorted(want - set(df["symbol"].unique()))
        if missing_symbols:
            raise ValueError(f"No input rows for symbols: {missing_symbols}")
    if df.empty:
        raise ValueError("No usable input rows after filtering")
    return df.sort_values(["symbol", "timestamp"])


def resample_inputs(df: pd.DataFrame, bar: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    close_parts: list[pd.Series] = []
    funding_parts: list[pd.Series] = []
    for symbol, g in df.groupby("symbol", sort=True):
        g = g.set_index("timestamp").sort_index()
        close = g["close"].resample(bar).last().dropna()
        funding = g["funding_rate"].resample(bar).last().reindex(close.index).fillna(0.0)
        close.name = symbol
        funding.name = symbol
        close_parts.append(close)
        funding_parts.append(funding)

    close_df = pd.concat(close_parts, axis=1).sort_index()
    funding_df = pd.concat(funding_parts, axis=1).reindex(close_df.index).fillna(0.0)
    return close_df, funding_df


def _scaled_signal(z: pd.Series) -> pd.Series:
    # Rohrbach/Baz-style bounded trend score; denominator scales extrema to +/-1.
    denom = math.sqrt(2.0) * math.exp(-0.5)
    out = z * np.exp(-(z**2) / 4.0) / denom
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def compute_symbol_signal(
    close: pd.Series,
    short_windows: list[int],
    long_windows: list[int],
    short_vol_window: int,
    long_signal_vol_window: int,
) -> pd.Series:
    if len(short_windows) != len(long_windows):
        raise ValueError("short EMA and long EMA window lists must have equal length")
    if close.dropna().empty:
        return pd.Series(np.nan, index=close.index, dtype=float)

    indexed = close / float(close.dropna().iloc[0])
    signals: list[pd.Series] = []
    price_vol = indexed.rolling(short_vol_window, min_periods=short_vol_window).std()

    for short_n, long_n in zip(short_windows, long_windows):
        short_ema = indexed.ewm(alpha=1.0 / float(short_n), adjust=False).mean()
        long_ema = indexed.ewm(alpha=1.0 / float(long_n), adjust=False).mean()
        x = short_ema - long_ema

        y = x / price_vol
        y = y.mask((x == 0.0) | (price_vol <= 0.0), 0.0)
        y = y.replace([np.inf, -np.inf], np.nan)

        y_vol = y.rolling(long_signal_vol_window, min_periods=long_signal_vol_window).std()
        z = y / y_vol
        z = z.mask((x == 0.0) | (price_vol <= 0.0) | (y_vol <= 0.0), 0.0)
        signals.append(_scaled_signal(z))

    return pd.concat(signals, axis=1).mean(axis=1)


def build_signal_matrix(
    close: pd.DataFrame,
    short_windows: list[int],
    long_windows: list[int],
    short_vol_window: int,
    long_signal_vol_window: int,
) -> pd.DataFrame:
    parts = {
        symbol: compute_symbol_signal(
            close[symbol],
            short_windows=short_windows,
            long_windows=long_windows,
            short_vol_window=short_vol_window,
            long_signal_vol_window=long_signal_vol_window,
        )
        for symbol in close.columns
    }
    return pd.DataFrame(parts, index=close.index)


def build_cross_sectional_weights(signals: pd.DataFrame, leg_count: int) -> pd.DataFrame:
    if leg_count <= 0:
        raise ValueError("--leg_count must be > 0")

    weights = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
    for ts, row in signals.iterrows():
        valid = row.dropna()
        valid = valid[valid != 0.0]
        if len(valid) < 2 * leg_count:
            continue
        ranked = valid.sort_values(ascending=False)
        longs = ranked.index[:leg_count]
        shorts = ranked.index[-leg_count:]
        if set(longs) & set(shorts):
            continue
        weights.loc[ts, longs] = 1.0 / (2.0 * leg_count)
        weights.loc[ts, shorts] = -1.0 / (2.0 * leg_count)
    return weights


def compute_strategy_returns(
    close: pd.DataFrame,
    funding_rate: pd.DataFrame,
    weights: pd.DataFrame,
    *,
    bar: str,
    fee_bps: float,
    slippage_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    fwd_returns = close.pct_change().shift(-1)
    weights = weights.reindex_like(close).fillna(0.0)
    gross = (weights * fwd_returns).sum(axis=1)

    bar_hours = _bar_seconds(bar) / 3600.0
    funding = funding_rate.reindex_like(close).fillna(0.0)
    # Positive funding means longs pay shorts. Approximate linear accrual per 8h period.
    funding_return = (-(weights * funding) * (bar_hours / 8.0)).sum(axis=1)

    prev_weights = weights.shift(1).fillna(0.0)
    turnover = (weights - prev_weights).abs().sum(axis=1)
    cost_return = -turnover * ((float(fee_bps) + float(slippage_bps)) / 1e4)

    out = pd.DataFrame(
        {
            "gross_return": gross,
            "funding_return": funding_return,
            "cost_return": cost_return,
            "net_return": gross + funding_return + cost_return,
            "turnover": turnover,
            "gross_exposure": weights.abs().sum(axis=1),
            "net_exposure": weights.sum(axis=1),
        },
        index=close.index,
    ).iloc[:-1]
    out["cum_gross_return"] = (1.0 + out["gross_return"]).cumprod() - 1.0
    out["cum_net_return"] = (1.0 + out["net_return"]).cumprod() - 1.0
    return out, weights.reindex(out.index)


def summarize_returns(returns: pd.DataFrame, bar: str, symbols: list[str], leg_count: int) -> dict[str, object]:
    active = returns[returns["gross_exposure"] > 0].copy()
    periods_per_year = (365.0 * 24.0 * 3600.0) / _bar_seconds(bar)
    net = returns["net_return"].fillna(0.0)
    gross = returns["gross_return"].fillna(0.0)
    equity = (1.0 + net).cumprod()
    drawdown = equity / equity.cummax() - 1.0

    def _ann_return(r: pd.Series) -> float:
        if r.empty:
            return 0.0
        total = float((1.0 + r.fillna(0.0)).prod())
        years = max(len(r) / periods_per_year, 1e-12)
        return total ** (1.0 / years) - 1.0

    def _sharpe(r: pd.Series) -> Optional[float]:
        std = float(r.std(ddof=0))
        if std <= 0:
            return None
        return float(r.mean() / std * math.sqrt(periods_per_year))

    return {
        "symbols": symbols,
        "n_symbols": len(symbols),
        "leg_count": leg_count,
        "start": str(returns.index.min()) if not returns.empty else None,
        "end": str(returns.index.max()) if not returns.empty else None,
        "bars": int(len(returns)),
        "active_bars": int(len(active)),
        "gross_total_return": float((1.0 + gross).prod() - 1.0),
        "net_total_return": float((1.0 + net).prod() - 1.0),
        "gross_annualized_return": _ann_return(gross),
        "net_annualized_return": _ann_return(net),
        "net_annualized_vol": float(net.std(ddof=0) * math.sqrt(periods_per_year)),
        "net_sharpe": _sharpe(net),
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "mean_period_net_return": float(net.mean()),
        "hit_rate": float((net > 0).mean()) if len(net) else 0.0,
        "avg_turnover": float(returns["turnover"].mean()),
        "avg_active_turnover": float(active["turnover"].mean()) if not active.empty else 0.0,
        "n_rebalances": int((returns["turnover"] > 0).sum()),
        "avg_gross_exposure": float(returns["gross_exposure"].mean()),
        "avg_net_exposure": float(returns["net_exposure"].mean()),
        "total_cost_return": float(returns["cost_return"].sum()),
        "total_funding_return": float(returns["funding_return"].sum()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cross-sectional EMA momentum backtest adapted from Chu/Chan/Zhang (2020)."
    )
    parser.add_argument("--inputs_csv", default="./backtest/backtest_inputs.csv")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol filter")
    parser.add_argument("--bar", default="1h", help="Pandas resample frequency, e.g. 1h, 4h, 1d")
    parser.add_argument("--short_ema_windows", default="8,16,32")
    parser.add_argument("--long_ema_windows", default="24,48,96")
    parser.add_argument("--short_vol_window", type=int, default=12)
    parser.add_argument("--long_signal_vol_window", type=int, default=168)
    parser.add_argument("--leg_count", type=int, default=3)
    parser.add_argument("--fee_bps", type=float, default=4.0)
    parser.add_argument("--slippage_bps", type=float, default=0.0)
    parser.add_argument("--out_dir", default="./backtest/results/cross_sectional_momentum")
    parser.add_argument("--run_name", default="")
    args = parser.parse_args()

    short_windows = _parse_int_list(args.short_ema_windows)
    long_windows = _parse_int_list(args.long_ema_windows)
    if len(short_windows) != len(long_windows):
        raise ValueError("--short_ema_windows and --long_ema_windows must have the same length")
    if args.short_vol_window <= 0 or args.long_signal_vol_window <= 0:
        raise ValueError("Normalization windows must be positive")

    symbols_filter = _parse_symbols(args.symbols)
    inputs = load_price_inputs(Path(args.inputs_csv), symbols=symbols_filter)
    close, funding_rate = resample_inputs(inputs, bar=args.bar)
    close = close.dropna(axis=1, how="all")
    if close.shape[1] < 2 * args.leg_count:
        raise ValueError(
            f"Need at least {2 * args.leg_count} symbols for leg_count={args.leg_count}; "
            f"found {close.shape[1]} symbols: {list(close.columns)}"
        )

    signals = build_signal_matrix(
        close,
        short_windows=short_windows,
        long_windows=long_windows,
        short_vol_window=args.short_vol_window,
        long_signal_vol_window=args.long_signal_vol_window,
    )
    weights = build_cross_sectional_weights(signals, leg_count=args.leg_count)
    returns, positions = compute_strategy_returns(
        close=close,
        funding_rate=funding_rate,
        weights=weights,
        bar=args.bar,
        fee_bps=args.fee_bps,
        slippage_bps=args.slippage_bps,
    )
    summary = summarize_returns(
        returns=returns,
        bar=args.bar,
        symbols=list(close.columns),
        leg_count=args.leg_count,
    )
    summary.update(
        {
            "inputs_csv": str(args.inputs_csv),
            "bar": args.bar,
            "short_ema_windows": short_windows,
            "long_ema_windows": long_windows,
            "short_vol_window": args.short_vol_window,
            "long_signal_vol_window": args.long_signal_vol_window,
            "fee_bps": args.fee_bps,
            "slippage_bps": args.slippage_bps,
        }
    )

    run_name = args.run_name.strip() or pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    returns_path = out_dir / f"{run_name}_returns.csv"
    positions_path = out_dir / f"{run_name}_positions.csv"
    signals_path = out_dir / f"{run_name}_signals.csv"
    summary_path = out_dir / f"{run_name}_summary.json"

    returns.to_csv(returns_path, index_label="timestamp")
    positions.to_csv(positions_path, index_label="timestamp")
    signals.reindex(returns.index).to_csv(signals_path, index_label="timestamp")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"[WROTE] returns={returns_path}")
    print(f"[WROTE] positions={positions_path}")
    print(f"[WROTE] signals={signals_path}")
    print(f"[WROTE] summary={summary_path}")


if __name__ == "__main__":
    main()
