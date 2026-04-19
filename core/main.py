from __future__ import annotations

import os

from dotenv import load_dotenv

from exchange.client import AsterClient
from execution.order import OrderPlacer
from runtime.config import (
    _default_symbol_params_from_args,
    build_arg_parser,
    load_symbol_runtime_config,
    validate_args,
)
from runtime.loop import run_live_runtime
from runtime.utils import _load_tick_size_by_symbol, _parse_hhmm_utc, _read_secret
from signals.strategy import Strategy, StrategyConfig

load_dotenv()


if __name__ == "__main__":
    parser = build_arg_parser()
    args = parser.parse_args()
    validate_args(args)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    entry_halt_min = _parse_hhmm_utc(args.entry_halt_utc)
    force_exit_min = _parse_hhmm_utc(args.force_exit_utc)
    symbol_params = load_symbol_runtime_config(
        config_file=args.config_current_file,
        symbols=symbols,
        defaults=_default_symbol_params_from_args(args),
    )
    trade_enabled_symbols = [sym for sym in symbols if bool(symbol_params[sym].get("enable_trading", True))]
    print(
        "[CONFIG_CURRENT] "
        + ("loaded " + args.config_current_file if args.config_current_file else "using CLI/env defaults")
    )
    for sym in symbols:
        print(f"[CONFIG_CURRENT] {sym} {symbol_params[sym]}")
    print(f"[CONFIG_CURRENT] trade_enabled_symbols={trade_enabled_symbols}")

    order_api_key = None
    order_api_secret = None
    if args.enable_trading:
        order_api_key = _read_secret(os.getenv("ORDER_API_KEY"))
        order_api_secret = _read_secret(os.getenv("ORDER_SECRET_KEY"))

    client = AsterClient(
        symbols=symbols,
        log_dir=args.log_dir,
        delete_logs=args.delete_logs,
        rest_key=order_api_key,
        rest_secret=order_api_secret,
    )
    tick_size_by_symbol = _load_tick_size_by_symbol(client.rest, symbols)
    if tick_size_by_symbol:
        print(f"[TICK_SIZE] {tick_size_by_symbol}")
    else:
        print("[TICK_SIZE] unavailable; spread blocker will use fallback --max_spread")

    strat_by_symbol = {}
    for sym in symbols:
        sp = symbol_params[sym]
        strat_by_symbol[sym] = Strategy(
            StrategyConfig(
                k=sp["k"],
                t_window=sp["T"],
                n=sp["n"],
                v_window=sp["V"],
                max_spread=sp["spread_max"],
                max_spread_ticks=args.max_spread_ticks,
                max_funding_abs_bps=sp["funding_max"],
                tick_size_by_symbol=tick_size_by_symbol,
            ),
            symbols=[sym],
        )

    order_placer = None
    if args.enable_trading:
        order_placer = OrderPlacer(api_key=str(order_api_key), api_secret=str(order_api_secret))
        if trade_enabled_symbols:
            risk_setup = order_placer.ensure_risk_setup(
                symbols=trade_enabled_symbols,
                leverage=args.target_leverage,
                margin_type="ISOLATED",
            )
            print(f"[RISK_SETUP] {risk_setup}")
        else:
            print("[RISK_SETUP] skipped; no symbols are trade-enabled in config_current")
        print("Live trading: ENABLED")
    else:
        print("Live trading: DISABLED")

    run_live_runtime(
        args=args,
        client=client,
        order_placer=order_placer,
        symbols=symbols,
        symbol_params=symbol_params,
        strat_by_symbol=strat_by_symbol,
        entry_halt_min=entry_halt_min,
        force_exit_min=force_exit_min,
    )
