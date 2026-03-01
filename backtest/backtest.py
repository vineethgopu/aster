from __future__ import annotations

import argparse
import gc
import itertools
import json
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import vectorbt as vbt
from numba import njit


REQUIRED_PARAM_KEYS = [
    "k",
    "T",
    "n",
    "V",
    "tp_bps",
    "sl_bps",
    "activation_bps",
    "activation_buffer_bps",
    "callback_bps",
    "min_tp_gap_bps",
    "spread_max",
    "funding_max",
]


def _to_float(x: object, key: str, symbol: str) -> float:
    try:
        return float(x)
    except Exception as e:
        raise ValueError(f"symbols.{symbol}.params.{key} contains non-numeric value: {x!r}") from e


def _validate_param_values(symbol: str, key: str, values: List[object]) -> List[object]:
    out: List[object] = []
    for v in values:
        fv = _to_float(v, key=key, symbol=symbol)
        if key in ("T", "V"):
            if fv <= 0 or int(fv) != fv:
                raise ValueError(f"symbols.{symbol}.params.{key} must contain positive integers")
            out.append(int(fv))
        elif key in ("activation_buffer_bps", "min_tp_gap_bps"):
            if fv < 0:
                raise ValueError(f"symbols.{symbol}.params.{key} must be >= 0")
            out.append(float(fv))
        else:
            if fv <= 0:
                raise ValueError(f"symbols.{symbol}.params.{key} must be > 0")
            out.append(float(fv))
    return out


@njit
def _adjust_sl_with_activation_nb(c, activation_stop, callback_stop):
    """
    Emulate exchange-style trailing activation:
    - Keep the base SL (sl_stop) until activation threshold is reached.
    - After activation, switch to trailing stop distance = callback_bps.
    """
    if c.position_now == 0:
        return c.curr_stop, c.curr_trail

    cb = callback_stop[c.col]
    if not np.isfinite(cb) or cb <= 0:
        cb = activation_stop[c.init_i, c.col]
        if cb <= 0:
            cb = c.curr_stop if c.curr_stop > 0 else 0.0001
    if c.curr_trail:
        return cb, True

    if c.init_price <= 0:
        return c.curr_stop, c.curr_trail

    activation = activation_stop[c.init_i, c.col]
    if activation <= 0:
        return cb, True

    direction = 1.0 if c.position_now > 0 else -1.0
    move = direction * (c.val_price_now / c.init_price - 1.0)
    if move >= activation:
        return cb, True
    return c.curr_stop, False


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

        normalized_space: Dict[str, List[object]] = {}
        for key in REQUIRED_PARAM_KEYS:
            normalized_space[key] = _validate_param_values(symbol, key, list(param_space[key]))

        keys = list(normalized_space.keys())
        combos = list(itertools.product(*(normalized_space[k] for k in keys)))
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
    open_px: pd.Series,
    high_px: pd.Series,
    low_px: pd.Series,
    tw_bid_px: pd.Series,
    tw_ask_px: pd.Series,
    opening_loss_bps: pd.Series,
    funding_bps: pd.Series,
    long_entries: pd.DataFrame,
    long_exits: pd.DataFrame,
    short_entries: pd.DataFrame,
    short_exits: pd.DataFrame,
    params: pd.DataFrame,
    fee_bps: float = 4.0,
    slippage_bps: float = 0.0,
) -> vbt.Portfolio:
    def _to_2d(s: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(
            np.repeat(s.values[:, None], len(params), axis=1),
            index=close.index,
            columns=params.index,
        )

    close_2d = _to_2d(close)
    open_2d = _to_2d(open_px)
    high_2d = _to_2d(high_px)
    low_2d = _to_2d(low_px)

    fees = fee_bps / 1e4
    tw_mid = 0.5 * (tw_bid_px + tw_ask_px)
    tw_mid = tw_mid.where(tw_mid > 0, close).fillna(close)
    price_2d = _to_2d(tw_mid)
    # Slippage in vectorbt is applied per order, so this half-spread component
    # impacts both entry and exit (roundtrip ~= one full spread, before extras).
    per_order_half_spread_slippage = ((tw_ask_px - tw_bid_px) / (2.0 * tw_mid)).clip(lower=0.0).fillna(0.0)
    slippage_2d = _to_2d(per_order_half_spread_slippage + (slippage_bps / 1e4))

    tp_bps_col = params["tp_bps"].astype(float).values[None, :]
    sl_bps_col = params["sl_bps"].astype(float).values[None, :]
    activation_bps_col = params["activation_bps"].astype(float).values[None, :]
    activation_buffer_bps_col = params["activation_buffer_bps"].astype(float).values[None, :]
    min_tp_gap_bps_col = params["min_tp_gap_bps"].astype(float).values[None, :]
    callback_bps_col = params["callback_bps"].astype(float).values

    opening_loss_mat = opening_loss_bps.reindex(close.index).astype(float).values[:, None]
    funding_abs_mat = np.abs(funding_bps.reindex(close.index).astype(float).values[:, None])

    # Activation floor per potential entry bar:
    # 2*taker_fee + opening_loss + |funding|/8 + user buffer.
    auto_activation_bps_mat = (
        (2.0 * fee_bps)
        + opening_loss_mat
        + (funding_abs_mat / 8.0)
        + activation_buffer_bps_col
    )
    activation_bps_mat = np.maximum(activation_bps_col, auto_activation_bps_mat)

    # Enforce TP above activation by configured minimum gap.
    tp_bps_mat = np.maximum(tp_bps_col, activation_bps_mat + np.maximum(min_tp_gap_bps_col, 0.0))
    tp_stop = tp_bps_mat / 1e4

    sl_stop = sl_bps_col / 1e4

    # Trailing callback in relative units (fraction), e.g. 6 bps => 0.0006.
    callback_stop = callback_bps_col / 1e4
    activation_stop = activation_bps_mat / 1e4
    sl_trail = False

    return vbt.Portfolio.from_signals(
        close=close_2d,
        price=price_2d,
        open=open_2d,
        high=high_2d,
        low=low_2d,
        entries=long_entries,
        exits=long_exits,
        short_entries=short_entries,
        short_exits=short_exits,
        fees=fees,
        slippage=slippage_2d,
        tp_stop=tp_stop,
        sl_stop=sl_stop,
        sl_trail=sl_trail,
        stop_entry_price="Price",
        stop_exit_price="Price",
        adjust_sl_func_nb=_adjust_sl_with_activation_nb,
        adjust_sl_args=(activation_stop, callback_stop),
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
    chunk_size: int,
) -> pd.DataFrame:
    inputs_df = inputs_df.set_index("timestamp").sort_index()
    t_values = symbol_cfg["T"].astype(int).tolist()
    v_values = symbol_cfg["V"].astype(int).tolist()
    vol_by_t, avg_by_v = _build_feature_maps(inputs_df, t_values=t_values, v_values=v_values)

    close = pd.to_numeric(inputs_df["close"], errors="coerce")
    open_px = pd.to_numeric(inputs_df.get("open", close), errors="coerce")
    high_px = pd.to_numeric(inputs_df.get("high", close), errors="coerce")
    low_px = pd.to_numeric(inputs_df.get("low", close), errors="coerce")
    ret_bps = pd.to_numeric(inputs_df["ret_bps"], errors="coerce")
    vol1m = pd.to_numeric(inputs_df["vol1m"], errors="coerce")
    spread_bps = pd.to_numeric(inputs_df["spread_bps"], errors="coerce")
    funding_bps = pd.to_numeric(inputs_df["funding_bps"], errors="coerce")
    opening_loss_bps = pd.to_numeric(inputs_df["opening_loss_bps"], errors="coerce")
    tw_bid_px = pd.to_numeric(inputs_df.get("tw_bid_px", inputs_df.get("bid_px", close)), errors="coerce")
    tw_ask_px = pd.to_numeric(inputs_df.get("tw_ask_px", inputs_df.get("ask_px", close)), errors="coerce")

    open_px = open_px.fillna(close)
    high_px = high_px.fillna(close)
    low_px = low_px.fillna(close)
    high_px = pd.concat([high_px, open_px, close], axis=1).max(axis=1)
    low_px = pd.concat([low_px, open_px, close], axis=1).min(axis=1)
    tw_bid_px = tw_bid_px.fillna(close)
    tw_ask_px = tw_ask_px.fillna(close)
    bad_spread = tw_bid_px > tw_ask_px
    if bad_spread.any():
        swap_bid = tw_bid_px.copy()
        tw_bid_px = tw_bid_px.where(~bad_spread, tw_ask_px)
        tw_ask_px = tw_ask_px.where(~bad_spread, swap_bid)

    valid = close.notna() & ret_bps.notna() & vol1m.notna() & spread_bps.notna() & funding_bps.notna() & opening_loss_bps.notna()
    close = close[valid]
    open_px = open_px[valid]
    high_px = high_px[valid]
    low_px = low_px[valid]
    ret_bps = ret_bps[valid]
    vol1m = vol1m[valid]
    spread_bps = spread_bps[valid]
    funding_bps = funding_bps[valid]
    opening_loss_bps = opening_loss_bps[valid]
    tw_bid_px = tw_bid_px[valid]
    tw_ask_px = tw_ask_px[valid]
    for key in list(vol_by_t.keys()):
        vol_by_t[key] = vol_by_t[key][valid]
    for key in list(avg_by_v.keys()):
        avg_by_v[key] = avg_by_v[key][valid]

    params_all = symbol_cfg.drop(columns=["symbol"]).copy().set_index("config_id")
    symbol = symbol_cfg["symbol"].iloc[0]
    n_cfg = len(params_all)
    if n_cfg == 0:
        return pd.DataFrame()

    if chunk_size <= 0:
        chunk_size = n_cfg

    ranked_chunks: List[pd.DataFrame] = []
    for i, start in enumerate(range(0, n_cfg, chunk_size), start=1):
        end = min(start + chunk_size, n_cfg)
        params = params_all.iloc[start:end]
        print(
            f"[CHUNK] symbol={symbol} chunk={i} start={start} end={end} size={len(params)} total_cfg={n_cfg}"
        )
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
            open_px=open_px,
            high_px=high_px,
            low_px=low_px,
            tw_bid_px=tw_bid_px,
            tw_ask_px=tw_ask_px,
            opening_loss_bps=opening_loss_bps,
            funding_bps=funding_bps,
            long_entries=le,
            long_exits=lx,
            short_entries=se,
            short_exits=sx,
            params=params,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
        )
        ranked = build_ranked_metrics(pf).join(params, how="left")
        ranked.insert(0, "symbol", symbol)
        ranked_chunks.append(ranked)

        # Release heavy arrays between chunks to lower peak memory.
        del le, lx, se, sx, pf, ranked, params
        gc.collect()

    all_ranked = pd.concat(ranked_chunks, axis=0)
    return all_ranked.sort_values("total_pnl", ascending=False)


def _parse_symbols_filter(raw: str) -> Optional[List[str]]:
    raw = str(raw or "").strip()
    if not raw:
        return None
    syms = [s.strip().upper() for s in raw.split(",") if s.strip()]
    return syms or None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run VectorBT grid backtest from config JSON and backtest_inputs.csv.")
    parser.add_argument("--config_file", type=str, default="./backtest/config_grid.json")
    parser.add_argument("--inputs_csv", type=str, default="")
    parser.add_argument("--out_ranked_csv", type=str, default="")
    parser.add_argument("--out_config_map_csv", type=str, default="")
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Optional comma-separated symbol filter, e.g. ETHUSDT or BTCUSDT,ETHUSDT",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=1000,
        help="Max number of configs processed per chunk per symbol (reduces memory).",
    )
    args = parser.parse_args()

    cfg = load_config(Path(args.config_file))
    symbol_cfg = cfg.get("symbols", {})
    if not isinstance(symbol_cfg, dict):
        raise ValueError("config.symbols must be an object keyed by symbol")
    cfg_map = build_config_map(symbol_cfg=symbol_cfg)
    symbols_filter = _parse_symbols_filter(args.symbols)
    if symbols_filter is not None:
        cfg_map = cfg_map[cfg_map["symbol"].isin(symbols_filter)].copy()
        if cfg_map.empty:
            raise ValueError(f"No configs left after --symbols filter: {symbols_filter}")

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
        ranked = _run_for_symbol(
            sym_inputs,
            sym_cfg,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            chunk_size=args.chunk_size,
        )
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
