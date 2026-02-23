from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import vectorbt as vbt


REQUIRED_PARAM_KEYS = [
    "k",
    "T",
    "n",
    "V",
    "tp_bps",
    "sl_bps",
    "callbackRate",
    "spread_max",
    "funding_max",
]


def load_config(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config must be a JSON object: {path}")
    return cfg


def build_config_map(symbol_cfg: Dict[str, Dict]) -> pd.DataFrame:
    if not isinstance(symbol_cfg, dict) or not symbol_cfg:
        raise ValueError("config.symbols must be a non-empty object: {SYMBOL: {params: {...}}}")

    out_parts: List[pd.DataFrame] = []
    for raw_symbol, cfg in symbol_cfg.items():
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            continue
        if not isinstance(cfg, dict):
            raise ValueError(f"symbols.{symbol} must be an object")
        param_space = cfg.get("params", {})
        if not isinstance(param_space, dict):
            raise ValueError(f"symbols.{symbol}.params must be an object of parameter lists")

        for key in REQUIRED_PARAM_KEYS:
            if key not in param_space:
                raise ValueError(f"Missing required param list: symbols.{symbol}.params.{key}")
            if not isinstance(param_space[key], list) or not param_space[key]:
                raise ValueError(f"symbols.{symbol}.params.{key} must be a non-empty list")

        keys = list(param_space.keys())
        combos = list(itertools.product(*(param_space[k] for k in keys)))
        df = pd.DataFrame(combos, columns=keys)
        df.insert(0, "symbol", symbol)
        out_parts.append(df)

    if not out_parts:
        raise ValueError("No valid symbols found in config.symbols")

    out = pd.concat(out_parts, ignore_index=True)
    out.insert(0, "config_id", [f"cfg_{i:04d}" for i in range(len(out))])
    return out


def write_config_map_csv(df: pd.DataFrame, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)


def _select_feature_matrix(
    feature_by_key: Dict[int, pd.Series],
    keys: pd.Series,
    index: pd.Index,
) -> np.ndarray:
    cols = []
    for k in keys.values:
        s = feature_by_key[int(k)].reindex(index).astype(float)
        cols.append(s.values)
    return np.column_stack(cols)


def build_signals(
    close: pd.Series,
    ret_bps: pd.Series,
    vol_rolling_by_t: Dict[int, pd.Series],
    vol1m: pd.Series,
    avg_vol_by_v: Dict[int, pd.Series],
    spread_bps: pd.Series,
    funding_bps: pd.Series,
    opening_loss_bps: pd.Series,
    params: pd.DataFrame,
):
    idx = close.index

    ret = ret_bps.reindex(idx).values[:, None]
    vol_mat = _select_feature_matrix(vol_rolling_by_t, params["T"], idx)
    cur_vol = vol1m.reindex(idx).values[:, None]
    avg_vol_mat = _select_feature_matrix(avg_vol_by_v, params["V"], idx)

    spread = spread_bps.reindex(idx).values[:, None]
    funding = funding_bps.reindex(idx).values[:, None]
    opening_loss = opening_loss_bps.reindex(idx).values[:, None]

    k = params["k"].values[None, :]
    n = params["n"].values[None, :]
    spread_max = params["spread_max"].values[None, :]
    funding_max = params["funding_max"].values[None, :]

    opening_loss_max = np.minimum(10.0, 5.0 + 2.0 * spread)
    blockers_ok = (
        (spread <= spread_max)
        & (np.abs(funding) <= funding_max)
        & (opening_loss <= opening_loss_max)
    )

    long_ind1 = ret > (k * vol_mat)
    short_ind1 = ret < -(k * vol_mat)
    ind2 = cur_vol > (n * avg_vol_mat)

    long_entries = long_ind1 & ind2 & blockers_ok
    short_entries = short_ind1 & ind2 & blockers_ok

    long_exits = short_entries
    short_exits = long_entries

    cols = params.index
    to_df = lambda arr: pd.DataFrame(arr, index=idx, columns=cols)
    return to_df(long_entries), to_df(long_exits), to_df(short_entries), to_df(short_exits)


def run_grid_backtest(
    close: pd.Series,
    long_entries: pd.DataFrame,
    long_exits: pd.DataFrame,
    short_entries: pd.DataFrame,
    short_exits: pd.DataFrame,
    params: pd.DataFrame,
    fee_bps: float = 4.0,
    slippage_bps: float = 0.0,
) -> vbt.Portfolio:
    close_2d = pd.DataFrame(
        np.repeat(close.values[:, None], len(params), axis=1),
        index=close.index,
        columns=params.index,
    )

    fees = fee_bps / 1e4
    slippage = slippage_bps / 1e4

    tp_stop = params["tp_bps"].values / 1e4 if "tp_bps" in params.columns else None
    sl_stop = params["sl_bps"].values / 1e4 if "sl_bps" in params.columns else None
    sl_trail = params["callbackRate"].values if "callbackRate" in params.columns else None

    return vbt.Portfolio.from_signals(
        close=close_2d,
        entries=long_entries,
        exits=long_exits,
        short_entries=short_entries,
        short_exits=short_exits,
        fees=fees,
        slippage=slippage,
        tp_stop=tp_stop,
        sl_stop=sl_stop,
        sl_trail=sl_trail,
        freq="1min",
    )


def build_ranked_metrics(pf: vbt.Portfolio) -> pd.DataFrame:
    metrics = pd.DataFrame(
        {
            "total_pnl": pf.total_profit(),
            "total_return": pf.total_return(),
            "n_trades": pf.trades.count(),
            "win_rate": pf.trades.win_rate(),
            "sharpe": pf.sharpe_ratio(),
            "sortino": pf.sortino_ratio(),
            "max_drawdown": pf.max_drawdown(),
            "avg_trade_pnl": pf.trades.pnl.mean(),
            "avg_win": pf.trades.winning.pnl.mean(),
            "avg_loss": pf.trades.losing.pnl.mean(),
            "avg_hold_bars": pf.trades.duration.mean(),
        }
    )
    return metrics.sort_values("total_pnl", ascending=False)


def _load_inputs_all(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise ValueError("Input CSV must contain timestamp column")
    if "symbol" not in df.columns:
        raise ValueError("Input CSV must contain symbol column")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return df.sort_values(["symbol", "timestamp"]).drop_duplicates(subset=["symbol", "timestamp"], keep="last")


def _build_feature_maps(df: pd.DataFrame, t_values: List[int], v_values: List[int]):
    vol_by_t: Dict[int, pd.Series] = {}
    avg_by_v: Dict[int, pd.Series] = {}

    for t in sorted(set(t_values)):
        col = f"rs_vol_{t}m_bps"
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        vol_by_t[t] = pd.to_numeric(df[col], errors="coerce")
    for v in sorted(set(v_values)):
        col = f"avg_vol_{v}m"
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        avg_by_v[v] = pd.to_numeric(df[col], errors="coerce")
    return vol_by_t, avg_by_v


def _run_for_symbol(
    inputs_df: pd.DataFrame,
    symbol_cfg: pd.DataFrame,
    fee_bps: float,
    slippage_bps: float,
) -> pd.DataFrame:
    inputs_df = inputs_df.set_index("timestamp").sort_index()
    t_values = symbol_cfg["T"].astype(int).tolist()
    v_values = symbol_cfg["V"].astype(int).tolist()
    vol_by_t, avg_by_v = _build_feature_maps(inputs_df, t_values=t_values, v_values=v_values)

    close = pd.to_numeric(inputs_df["close"], errors="coerce")
    ret_bps = pd.to_numeric(inputs_df["ret_bps"], errors="coerce")
    vol1m = pd.to_numeric(inputs_df["vol1m"], errors="coerce")
    spread_bps = pd.to_numeric(inputs_df["spread_bps"], errors="coerce")
    funding_bps = pd.to_numeric(inputs_df["funding_bps"], errors="coerce")
    opening_loss_bps = pd.to_numeric(inputs_df["opening_loss_bps"], errors="coerce")

    valid = close.notna() & ret_bps.notna() & vol1m.notna() & spread_bps.notna() & funding_bps.notna() & opening_loss_bps.notna()
    close = close[valid]
    ret_bps = ret_bps[valid]
    vol1m = vol1m[valid]
    spread_bps = spread_bps[valid]
    funding_bps = funding_bps[valid]
    opening_loss_bps = opening_loss_bps[valid]
    for key in list(vol_by_t.keys()):
        vol_by_t[key] = vol_by_t[key][valid]
    for key in list(avg_by_v.keys()):
        avg_by_v[key] = avg_by_v[key][valid]

    params = symbol_cfg.drop(columns=["symbol"]).copy().set_index("config_id")
    le, lx, se, sx = build_signals(
        close=close,
        ret_bps=ret_bps,
        vol_rolling_by_t=vol_by_t,
        vol1m=vol1m,
        avg_vol_by_v=avg_by_v,
        spread_bps=spread_bps,
        funding_bps=funding_bps,
        opening_loss_bps=opening_loss_bps,
        params=params,
    )
    pf = run_grid_backtest(
        close=close,
        long_entries=le,
        long_exits=lx,
        short_entries=se,
        short_exits=sx,
        params=params,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
    )
    ranked = build_ranked_metrics(pf).join(params, how="left")
    ranked.insert(0, "symbol", symbol_cfg["symbol"].iloc[0])
    return ranked


def main() -> None:
    parser = argparse.ArgumentParser(description="Run VectorBT grid backtest from config JSON and backtest_inputs.csv.")
    parser.add_argument("--config_file", type=str, default="./backtest/config_grid.json")
    parser.add_argument("--inputs_csv", type=str, default="")
    parser.add_argument("--out_ranked_csv", type=str, default="")
    parser.add_argument("--out_config_map_csv", type=str, default="")
    args = parser.parse_args()

    cfg = load_config(Path(args.config_file))
    symbol_cfg = cfg.get("symbols", {})
    if not isinstance(symbol_cfg, dict):
        raise ValueError("config.symbols must be an object keyed by symbol")
    cfg_map = build_config_map(symbol_cfg=symbol_cfg)

    inputs_csv = args.inputs_csv.strip() or str(cfg.get("inputs_csv", "./backtest/backtest_inputs.csv"))
    inputs_path = Path(inputs_csv)
    output_dir = Path(str(cfg.get("output_dir", "./backtest/results")))
    out_ranked_csv = args.out_ranked_csv.strip() or str(output_dir / "ranked_metrics.csv")
    out_config_map_csv = args.out_config_map_csv.strip() or str(output_dir / "config_mapping.csv")
    fee_bps = float(cfg.get("fee_bps", 4.0))
    slippage_bps = float(cfg.get("slippage_bps", 0.0))

    write_config_map_csv(cfg_map, csv_path=Path(out_config_map_csv))
    all_inputs = _load_inputs_all(inputs_path)

    ranked_parts: List[pd.DataFrame] = []
    for sym in cfg_map["symbol"].drop_duplicates().tolist():
        sym_inputs = all_inputs[all_inputs["symbol"] == sym].copy()
        if sym_inputs.empty:
            print(f"[SKIP] {sym}: no rows in inputs CSV")
            continue
        sym_cfg = cfg_map[cfg_map["symbol"] == sym].copy()
        ranked = _run_for_symbol(sym_inputs, sym_cfg, fee_bps=fee_bps, slippage_bps=slippage_bps)
        ranked_parts.append(ranked)
        print(f"\n=== {sym} ===")
        print(ranked)
        print(f"[SUMMARY] symbol={sym} n_configs={len(sym_cfg)} rows={len(sym_inputs)}")

    if not ranked_parts:
        raise ValueError("No symbols produced ranked output. Check inputs CSV coverage vs config.symbols.")

    all_ranked = pd.concat(ranked_parts, axis=0).sort_values(["symbol", "total_pnl"], ascending=[True, False])
    out = Path(out_ranked_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    all_ranked.to_csv(out)
    print(f"\n[SUMMARY] wrote ranked metrics to {out}")
    print(f"[SUMMARY] wrote config map csv to {out_config_map_csv}")


if __name__ == "__main__":
    main()
