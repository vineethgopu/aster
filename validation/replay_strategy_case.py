from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_DIR = REPO_ROOT / "core"
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))

from strategy import Strategy, StrategyConfig  # noqa: E402


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _load_symbol_config(config_path: Path, symbol: str) -> Dict[str, Any]:
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    sym = str(symbol).upper()
    if not isinstance(raw, dict) or sym not in raw or not isinstance(raw[sym], dict):
        raise ValueError(f"{config_path} does not contain a config block for {sym}")
    return raw[sym]


def _load_case_manifest(case_dir: Path) -> Dict[str, Any]:
    manifest_path = case_dir / "case_manifest.json"
    if not manifest_path.exists():
        return {}
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _load_inputs(case_dir: Path, symbol: str) -> pd.DataFrame:
    inputs = case_dir / "inputs"
    kline = pd.read_csv(inputs / "kline.csv")
    book = pd.read_csv(inputs / "book_ticker.csv")
    mark = pd.read_csv(inputs / "mark_price.csv")

    for frame in (kline, book, mark):
        frame["symbol"] = frame["symbol"].astype(str).str.upper()

    kline = kline[kline["symbol"] == symbol].copy()
    book = book[book["symbol"] == symbol].copy()
    mark = mark[mark["symbol"] == symbol].copy()
    if kline.empty or book.empty or mark.empty:
        raise ValueError(f"Missing required fixture rows for {symbol}")

    merged = (
        kline.merge(book, on=["ts_unix_ms", "symbol"], how="inner", suffixes=("_k", "_b"))
        .merge(mark, on=["ts_unix_ms", "symbol"], how="inner", suffixes=("", "_m"))
        .sort_values("ts_unix_ms")
        .reset_index(drop=True)
    )
    return merged


def _compute_runtime_exit_levels(
    cfg: Dict[str, Any],
    decision: Dict[str, Any],
    funding: Dict[str, Any],
) -> tuple[float, float]:
    blockers = decision.get("blockers") or {}
    opening_loss_bps = float(max(0.0, blockers.get("opening_loss_bps") or 0.0))
    funding_bps = abs(float(funding.get("funding_rate") or 0.0) * 1e4)
    activation_auto_bps = (2.0 * 4.0) + opening_loss_bps + (funding_bps / 8.0) + float(cfg["activation_buffer_bps"])
    activation_bps = max(float(cfg["activation_bps"]), activation_auto_bps)
    tp_bps = max(float(cfg["tp_bps"]), activation_bps + max(0.0, float(cfg["min_tp_gap_bps"])))
    return activation_bps, tp_bps


def _open_trade(
    row: pd.Series,
    decision: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    side = str(decision["side"])
    entry_price = float(row["ask_px"]) if side == "BUY" else float(row["bid_px"])
    funding = {
        "funding_rate": row["funding_rate"],
    }
    activation_bps, tp_bps = _compute_runtime_exit_levels(cfg=cfg, decision=decision, funding=funding)

    if side == "BUY":
        tp_px = entry_price * (1.0 + tp_bps / 1e4)
        sl_px = entry_price * (1.0 - float(cfg["sl_bps"]) / 1e4)
        activation_px = entry_price * (1.0 + activation_bps / 1e4)
    else:
        tp_px = entry_price * (1.0 - tp_bps / 1e4)
        sl_px = entry_price * (1.0 + float(cfg["sl_bps"]) / 1e4)
        activation_px = entry_price * (1.0 - activation_bps / 1e4)

    return {
        "entry_time_ms": int(row["ts_unix_ms"]),
        "entry_time_utc": str(row["ts_dt_utc"]),
        "side": side,
        "entry_price": entry_price,
        "ret_bps": _safe_float(decision.get("ret_bps")),
        "rs_vol_bps": _safe_float(decision.get("rs_vol_bps")),
        "avg_base_vol": _safe_float(decision.get("avg_base_vol")),
        "activation_bps": activation_bps,
        "tp_bps": tp_bps,
        "sl_bps": float(cfg["sl_bps"]),
        "callback_bps": float(cfg["callback_bps"]),
        "tp_px": tp_px,
        "sl_px": sl_px,
        "activation_px": activation_px,
        "best_mark": float(row["mark_px"]),
        "trailing_armed": False,
    }


def _maybe_close_trade(open_trade: Dict[str, Any], row: pd.Series) -> Optional[Dict[str, Any]]:
    side = str(open_trade["side"])
    mark_px = float(row["mark_px"])
    ts_ms = int(row["ts_unix_ms"])
    callback_frac = float(open_trade["callback_bps"]) / 1e4

    if side == "BUY":
        open_trade["best_mark"] = max(float(open_trade["best_mark"]), mark_px)
        if mark_px >= float(open_trade["tp_px"]):
            return {"exit_reason": "TP", "exit_time_ms": ts_ms, "exit_price": mark_px}
        if mark_px <= float(open_trade["sl_px"]):
            return {"exit_reason": "SL", "exit_time_ms": ts_ms, "exit_price": mark_px}
        if mark_px >= float(open_trade["activation_px"]):
            open_trade["trailing_armed"] = True
        if bool(open_trade["trailing_armed"]) and mark_px <= float(open_trade["best_mark"]) * (1.0 - callback_frac):
            return {"exit_reason": "TSL", "exit_time_ms": ts_ms, "exit_price": mark_px}
        return None

    open_trade["best_mark"] = min(float(open_trade["best_mark"]), mark_px)
    if mark_px <= float(open_trade["tp_px"]):
        return {"exit_reason": "TP", "exit_time_ms": ts_ms, "exit_price": mark_px}
    if mark_px >= float(open_trade["sl_px"]):
        return {"exit_reason": "SL", "exit_time_ms": ts_ms, "exit_price": mark_px}
    if mark_px <= float(open_trade["activation_px"]):
        open_trade["trailing_armed"] = True
    if bool(open_trade["trailing_armed"]) and mark_px >= float(open_trade["best_mark"]) * (1.0 + callback_frac):
        return {"exit_reason": "TSL", "exit_time_ms": ts_ms, "exit_price": mark_px}
    return None


def _finalize_trade(open_trade: Dict[str, Any], close_event: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        key: value
        for key, value in open_trade.items()
        if key not in {"tp_px", "sl_px", "activation_px", "best_mark", "trailing_armed", "callback_bps", "sl_bps"}
    }
    out.update(close_event)
    out["duration_s"] = max(0.0, (int(close_event["exit_time_ms"]) - int(open_trade["entry_time_ms"])) / 1000.0)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case-dir", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--reentry-cooldown-min", type=float, default=None)
    args = parser.parse_args()

    symbol = str(args.symbol).upper()
    case_dir = Path(args.case_dir)
    config_path = Path(args.config)
    cfg = _load_symbol_config(config_path, symbol)
    manifest = _load_case_manifest(case_dir)
    rows = _load_inputs(case_dir, symbol)
    runtime_assumptions = manifest.get("runtime_assumptions") if isinstance(manifest.get("runtime_assumptions"), dict) else {}
    reentry_cooldown_min = (
        float(args.reentry_cooldown_min)
        if args.reentry_cooldown_min is not None
        else float(runtime_assumptions.get("reentry_cooldown_min", 10.0))
    )
    cooldown_ms = int(max(0.0, reentry_cooldown_min) * 60_000)

    strategy = Strategy(
        StrategyConfig(
            k=float(cfg["k"]),
            t_window=int(cfg["T"]),
            n=float(cfg["n"]),
            v_window=int(cfg["V"]),
            max_spread=float(cfg["spread_max"]),
            max_spread_ticks=2.0,
            max_funding_abs_bps=float(cfg["funding_max"]),
            tick_size_by_symbol={},
        ),
        symbols=[symbol],
    )

    raw_entry_signals = 0
    lifecycle_trades = []
    open_trade: Optional[Dict[str, Any]] = None
    cooldown_until_ms = 0

    for idx, row in rows.iterrows():
        bars = {
            "open": row["k1_open"],
            "high": row["k1_high"],
            "low": row["k1_low"],
            "close": row["k1_close"],
            "base_vol": row["k1_base_vol"],
            "quote_vol": row["k1_quote_vol"],
            "num_trades": row["k1_trades"],
            "is_closed": bool(row["k1_closed"]),
            "start_time_ms": int(row["k1_start_ms"]),
            "close_time_ms": int(row["k1_close_ms"]),
        }
        bbo = {
            "bid_px": row["bid_px"],
            "bid_qty": row["bid_qty"],
            "ask_px": row["ask_px"],
            "ask_qty": row["ask_qty"],
            "spread": row["spread"],
            "mid": row["mid"],
            "imbalance": row["imbalance"],
            "weighted_mid": row["weighted_mid"],
        }
        funding = {
            "mark_px": row["mark_px"],
            "index_px": row["index_px"],
            "funding_rate": row["funding_rate"],
            "next_funding_time_ms": row["next_funding_time_ms"],
            "mark_index_bps": row["mark_index_bps"],
        }
        decision = strategy.on_second(
            symbol=symbol,
            bars_1m=bars,
            bbo=bbo,
            funding=funding,
            now_ms=int(row["ts_unix_ms"]),
        )
        ts_ms = int(row["ts_unix_ms"])

        if open_trade is not None:
            close_event = _maybe_close_trade(open_trade, row)
            if close_event is not None:
                lifecycle_trades.append(_finalize_trade(open_trade, close_event))
                open_trade = None
                cooldown_until_ms = ts_ms + cooldown_ms
            continue

        if not decision or not decision.get("enter") or decision.get("side") not in {"BUY", "SELL"}:
            continue

        raw_entry_signals += 1
        if ts_ms < cooldown_until_ms:
            continue
        open_trade = _open_trade(row=row, decision=decision, cfg=cfg)

    if open_trade is not None:
        last_row = rows.iloc[-1]
        lifecycle_trades.append(
            _finalize_trade(
                open_trade,
                {
                    "exit_reason": "WINDOW_END",
                    "exit_time_ms": int(last_row["ts_unix_ms"]),
                    "exit_price": float(last_row["mark_px"]),
                },
            )
        )

    print(f"[CASE] symbol={symbol} config={config_path}")
    print(
        f"[CASE] rows={len(rows)} raw_entry_signals={raw_entry_signals} "
        f"simulated_trades={len(lifecycle_trades)} reentry_cooldown_min={reentry_cooldown_min:g}"
    )
    for row in lifecycle_trades[:20]:
        print(json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    main()
