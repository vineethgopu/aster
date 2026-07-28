from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

from exchange.client import AsterClient
from execution.order import OrderPlacer
from runtime.portfolio_loop import run_portfolio_runtime
from runtime.utils import _read_secret, _to_bool
from signals.cross_sectional_momentum import CrossSectionalMomentumStrategy


load_dotenv()


def _load_json(path: str | Path) -> Dict[str, Any]:
    p = Path(path).expanduser()
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Config must be a JSON object: {p}")
    return raw


def _runtime_cfg(raw: Dict[str, Any]) -> Dict[str, Any]:
    cfg = raw.get("portfolio_runtime", {})
    if not isinstance(cfg, dict):
        raise ValueError("portfolio_runtime must be a JSON object")
    return cfg


def _optional_bool(raw: str) -> bool | None:
    s = str(raw or "").strip()
    if not s:
        return None
    return _to_bool(s)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run cross-sectional momentum portfolio strategy.")
    parser.add_argument("--config_file", default="./config_cross_sectional_momentum.json")
    parser.add_argument("--log_dir", default=os.getenv("ASTER_LOG_DIR", "./logs"))
    parser.add_argument("--poll_time", type=int, default=int(os.getenv("ASTER_POLL_TIME", "600")))
    parser.add_argument("--delete_logs", type=_to_bool, default=_to_bool(os.getenv("ASTER_DELETE_LOGS", "false")))
    parser.add_argument("--update_logs", type=_to_bool, default=_to_bool(os.getenv("ASTER_UPDATE_LOGS", "true")))
    parser.add_argument(
        "--enable_trading",
        default="",
        help="Optional true/false override. Live orders require this true and config enable_trading=true.",
    )
    parser.add_argument("--target_leverage", type=int, default=0)
    parser.add_argument("--margin_type", default="")
    return parser


if __name__ == "__main__":
    parser = build_arg_parser()
    args = parser.parse_args()
    raw_cfg = _load_json(args.config_file)
    runtime_cfg = _runtime_cfg(raw_cfg)
    strategy = CrossSectionalMomentumStrategy.from_json_file(args.config_file)
    symbols = strategy.cfg.symbols

    cli_enable = _optional_bool(args.enable_trading)
    enable_trading = bool(strategy.cfg.enable_trading if cli_enable is None else cli_enable)
    # Require the config to opt in as well, so a CLI typo cannot trade a disabled config.
    live_orders_enabled = bool(enable_trading and strategy.cfg.enable_trading)

    target_leverage = int(args.target_leverage or runtime_cfg.get("target_leverage", 3))
    margin_type = str(args.margin_type or runtime_cfg.get("margin_type", "ISOLATED")).upper()
    daily_drawdown_blocker_pct = float(runtime_cfg.get("daily_drawdown_blocker_pct", 5.0))
    margin_safety_multiple = float(runtime_cfg.get("margin_safety_multiple", 1.2))
    dry_run_equity_usd = float(runtime_cfg.get("dry_run_equity_usd", 850.0))

    print(f"[PORTFOLIO_CONFIG] file={args.config_file}")
    print(f"[PORTFOLIO_CONFIG] symbols={symbols}")
    print(
        "[PORTFOLIO_CONFIG] "
        f"bar_minutes={strategy.cfg.bar_minutes} leg_count={strategy.cfg.leg_count} "
        f"target_gross_exposure={strategy.cfg.target_gross_exposure} "
        f"max_symbol_weight={strategy.cfg.max_symbol_weight} weighting={strategy.cfg.weighting}"
    )
    print(
        "[PORTFOLIO_EXPOSURE] "
        "gross_notional = account_equity * target_gross_exposure; "
        "per_symbol_abs_notional ~= gross_notional / (2 * leg_count)"
    )
    print(
        f"[PORTFOLIO_TRADING] cli_enable={cli_enable} "
        f"config_enable={strategy.cfg.enable_trading} live_orders_enabled={live_orders_enabled}"
    )

    order_api_key = None
    order_api_secret = None
    if live_orders_enabled:
        order_api_key = _read_secret(os.getenv("ORDER_API_KEY"))
        order_api_secret = _read_secret(os.getenv("ORDER_SECRET_KEY"))

    client = AsterClient(
        symbols=symbols,
        log_dir=args.log_dir,
        delete_logs=args.delete_logs,
        rest_key=order_api_key,
        rest_secret=order_api_secret,
    )

    order_placer = None
    if live_orders_enabled:
        order_placer = OrderPlacer(api_key=str(order_api_key), api_secret=str(order_api_secret))
        risk_setup = order_placer.ensure_risk_setup(
            symbols=symbols,
            leverage=target_leverage,
            margin_type=margin_type,
        )
        print(f"[PORTFOLIO_RISK_SETUP] {risk_setup}")
        print("Cross-sectional portfolio trading: ENABLED")
    else:
        print("Cross-sectional portfolio trading: DRY_RUN")

    run_portfolio_runtime(
        client=client,
        order_placer=order_placer,
        strategy=strategy,
        log_dir=args.log_dir,
        poll_time=args.poll_time,
        update_logs=args.update_logs,
        enable_trading=live_orders_enabled,
        daily_drawdown_blocker_pct=daily_drawdown_blocker_pct,
        margin_safety_multiple=margin_safety_multiple,
        dry_run_equity_usd=dry_run_equity_usd,
    )
