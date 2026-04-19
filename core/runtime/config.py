from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from .utils import _to_bool


_SYMBOL_PARAM_KEYS = {
    "k",
    "T",
    "enable_trading",
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
}
_SYMBOL_PARAM_INT_KEYS = {"T", "V"}
_SYMBOL_PARAM_BOOL_KEYS = {"enable_trading"}
_SYMBOL_PARAM_FLOAT_KEYS = _SYMBOL_PARAM_KEYS - _SYMBOL_PARAM_INT_KEYS - _SYMBOL_PARAM_BOOL_KEYS


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("--symbols", "-s", type=str, default="BTCUSDT")
    parser.add_argument("--log_dir", "-l", type=str, default="./logs")
    parser.add_argument("--poll_time", "-t", type=int, default=600)
    parser.add_argument("--delete_logs", "-d", type=_to_bool, default=False)
    parser.add_argument("--update_logs", type=_to_bool, default=True)

    parser.add_argument("--k", type=float, default=1.3)
    parser.add_argument("--T", type=int, default=30)
    parser.add_argument("--n", type=float, default=1.3)
    parser.add_argument("--V", type=int, default=30)
    parser.add_argument("--max_spread", type=float, default=0.2)
    parser.add_argument("--max_spread_ticks", type=float, default=2.0)
    parser.add_argument("--max_funding_abs_bps", type=float, default=1.5)

    parser.add_argument("--enable_trading", type=_to_bool, default=False)
    parser.add_argument("--target_leverage", type=int, default=25)
    parser.add_argument("--risk_pct", type=float, default=1.0)
    parser.add_argument("--order_notional", type=float, default=None)
    parser.add_argument("--trade_alert_email", type=_to_bool, default=True)
    parser.add_argument("--taker_fee_bps", type=float, default=4.0)
    parser.add_argument("--take_profit_bps", type=float, default=20.0)
    parser.add_argument("--stop_loss_bps", type=float, default=12.0)
    parser.add_argument("--trailing_activation_bps", type=float, default=8.0)
    parser.add_argument("--trailing_activation_buffer_bps", type=float, default=0.5)
    parser.add_argument("--trailing_callback_bps", type=float, default=6.0)
    parser.add_argument("--min_take_profit_gap_bps", type=float, default=4.0)
    parser.add_argument("--margin_safety_multiple", type=float, default=1.2)
    parser.add_argument("--daily_drawdown_blocker_pct", type=float, default=5.0)
    parser.add_argument("--reentry_cooldown_min", type=int, default=10)

    parser.add_argument("--entry_halt_utc", type=str, default="23:00")
    parser.add_argument("--force_exit_utc", type=str, default="23:50")
    parser.add_argument("--config_current_file", type=str, default="")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.take_profit_bps <= 0:
        raise ValueError("--take_profit_bps must be > 0")
    if args.stop_loss_bps <= 0:
        raise ValueError("--stop_loss_bps must be > 0")
    if args.trailing_activation_bps <= 0:
        raise ValueError("--trailing_activation_bps must be > 0")
    if args.trailing_activation_buffer_bps < 0:
        raise ValueError("--trailing_activation_buffer_bps must be >= 0")
    if args.trailing_callback_bps <= 0:
        raise ValueError("--trailing_callback_bps must be > 0")
    if args.min_take_profit_gap_bps < 0:
        raise ValueError("--min_take_profit_gap_bps must be >= 0")
    if args.daily_drawdown_blocker_pct <= 0 or args.daily_drawdown_blocker_pct >= 100:
        raise ValueError("--daily_drawdown_blocker_pct must be in (0, 100)")
    if args.risk_pct <= 0:
        raise ValueError("--risk_pct must be > 0")
    if args.target_leverage <= 0:
        raise ValueError("--target_leverage must be > 0")
    if args.order_notional is not None and args.order_notional <= 0:
        raise ValueError("--order_notional must be > 0 when provided")
    if args.max_spread_ticks <= 0:
        raise ValueError("--max_spread_ticks must be > 0")


def _default_symbol_params_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "k": float(args.k),
        "T": int(args.T),
        "enable_trading": bool(args.enable_trading),
        "n": float(args.n),
        "V": int(args.V),
        "tp_bps": float(args.take_profit_bps),
        "sl_bps": float(args.stop_loss_bps),
        "activation_bps": float(args.trailing_activation_bps),
        "activation_buffer_bps": float(args.trailing_activation_buffer_bps),
        "callback_bps": float(args.trailing_callback_bps),
        "min_tp_gap_bps": float(args.min_take_profit_gap_bps),
        "spread_max": float(args.max_spread),
        "funding_max": float(args.max_funding_abs_bps),
    }


def _cast_bool_value(key: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"{key} must be a boolean-like value, got: {value!r}")


def _cast_symbol_param_value(key: str, value: Any) -> Any:
    if key in _SYMBOL_PARAM_INT_KEYS:
        return int(value)
    if key in _SYMBOL_PARAM_BOOL_KEYS:
        return _cast_bool_value(key, value)
    if key in _SYMBOL_PARAM_FLOAT_KEYS:
        return float(value)
    return value


def _validate_symbol_params(symbol: str, p: Dict[str, Any]) -> None:
    if p["k"] <= 0:
        raise ValueError(f"{symbol}: k must be > 0")
    if p["T"] <= 0:
        raise ValueError(f"{symbol}: T must be > 0")
    if not isinstance(p["enable_trading"], bool):
        raise ValueError(f"{symbol}: enable_trading must be boolean")
    if p["n"] <= 0:
        raise ValueError(f"{symbol}: n must be > 0")
    if p["V"] <= 0:
        raise ValueError(f"{symbol}: V must be > 0")
    if p["tp_bps"] <= 0:
        raise ValueError(f"{symbol}: tp_bps must be > 0")
    if p["sl_bps"] <= 0:
        raise ValueError(f"{symbol}: sl_bps must be > 0")
    if p["activation_bps"] <= 0:
        raise ValueError(f"{symbol}: activation_bps must be > 0")
    if p["activation_buffer_bps"] < 0:
        raise ValueError(f"{symbol}: activation_buffer_bps must be >= 0")
    if p["callback_bps"] <= 0:
        raise ValueError(f"{symbol}: callback_bps must be > 0")
    if p["min_tp_gap_bps"] < 0:
        raise ValueError(f"{symbol}: min_tp_gap_bps must be >= 0")
    if p["spread_max"] <= 0:
        raise ValueError(f"{symbol}: spread_max must be > 0")
    if p["funding_max"] <= 0:
        raise ValueError(f"{symbol}: funding_max must be > 0")


def load_symbol_runtime_config(
    config_file: str,
    symbols: list[str],
    defaults: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    out = {sym: dict(defaults) for sym in symbols}
    config_file = str(config_file or "").strip()
    if not config_file:
        for sym in symbols:
            _validate_symbol_params(sym, out[sym])
        return out

    p = Path(config_file).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"--config_current_file not found: {p}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"config_current_file must be a JSON object: {p}")

    ignored_symbols: list[str] = []
    for raw_sym, patch in raw.items():
        sym = str(raw_sym).strip().upper()
        if sym not in out:
            ignored_symbols.append(sym)
            continue
        if not isinstance(patch, dict):
            raise ValueError(f"{p}: {sym} must be an object")
        missing = [k for k in _SYMBOL_PARAM_KEYS if k not in patch]
        if missing:
            raise ValueError(f"{p}: {sym} missing required keys: {missing}")
        sym_cfg: Dict[str, Any] = {}
        for k, v in patch.items():
            if k not in _SYMBOL_PARAM_KEYS:
                raise ValueError(f"{p}: unsupported key for {sym}: {k}")
            sym_cfg[k] = _cast_symbol_param_value(k, v)
        out[sym] = sym_cfg

    if ignored_symbols:
        print(f"[CONFIG_CURRENT] ignoring non-runtime symbols: {sorted(ignored_symbols)}")

    for sym in symbols:
        if sym not in raw:
            raise ValueError(f"{p}: missing symbol section for runtime symbol {sym}")

    for sym in symbols:
        _validate_symbol_params(sym, out[sym])
    return out
