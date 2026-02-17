from __future__ import annotations

import itertools
from typing import Dict

import numpy as np
import pandas as pd
import vectorbt as vbt


def make_param_grid(param_space: Dict[str, list]) -> pd.DataFrame:
    keys = list(param_space.keys())
    combos = list(itertools.product(*(param_space[k] for k in keys)))
    df = pd.DataFrame(combos, columns=keys)
    df["config_id"] = [f"cfg_{i:04d}" for i in range(len(df))]
    return df.set_index("config_id")


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

    # signal-flip exits
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


def plot_top_n(pf: vbt.Portfolio, ranked: pd.DataFrame, n: int = 3) -> None:
    top_cols = ranked.head(n).index
    for col in top_cols:
        print(f"\n=== {col} ===")
        pf[col].plot().show()
        pf[col].drawdowns.plot().show()
        pf[col].trades.plot().show()


if __name__ == "__main__":
    # Sample executable usage with synthetic data.
    np.random.seed(7)

    n = 2_000


    param_space = {
        "k": [1.0, 1.2, 1.4, 1.6, 1.8, 2.0],
        "T": [10, 30, 60],
        "n": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5],
        "V": [10, 30, 60],
        "tp_bps": [8.0, 12.0],
        "sl_bps": [8.0, 12.0],
        "callbackRate": [0.002, 0.004],
    }
    params = make_param_grid(param_space)

    le, lx, se, sx = build_signals(
        close=close,
        ret_bps=ret_bps,
        vol_rolling_by_t=vol_rolling_by_t,
        vol1m=vol1m,
        avg_vol_by_v=avg_vol_by_v,
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
        fee_bps=4.0,
        slippage_bps=0.2,
    )

    ranked = build_ranked_metrics(pf).join(params, how="left")
    print(ranked.head(20))

    # Comment out if running in a headless environment.
    plot_top_n(pf, ranked, n=3)
