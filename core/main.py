from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Dict

from client import AsterClient
from order import OrderPlacer, PositionState
from strategy import Strategy, StrategyConfig


def _to_bool(s: str) -> bool:
    return str(s).strip().lower() == "true"


def _read_secret(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def _now_ms() -> int:
    return int(time.time() * 1000)


if __name__ == "__main__":
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
    parser.add_argument("--max_funding_abs_bps", type=float, default=1.5)

    parser.add_argument("--enable_trading", type=_to_bool, default=False)
    parser.add_argument("--api_key_path", type=str, default="./api/api.txt")
    parser.add_argument("--api_secret_path", type=str, default="./api/secret.txt")
    parser.add_argument("--order_notional", type=float, default=5.0)
    parser.add_argument("--taker_fee_bps", type=float, default=4.0)
    parser.add_argument("--take_profit_mult", type=float, default=3.0)
    parser.add_argument("--stop_loss_bps", type=float, default=-1.0)
    parser.add_argument("--trailing_activation_frac", type=float, default=0.5)
    parser.add_argument("--trailing_callback_rate", type=float, default=-1.0)
    parser.add_argument("--margin_safety_multiple", type=float, default=1.2)
    parser.add_argument("--reentry_cooldown_min", type=int, default=10)

    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    client = AsterClient(symbols=symbols, log_dir=args.log_dir, delete_logs=args.delete_logs)
    strat = Strategy(
        StrategyConfig(
            k=args.k,
            t_window=args.T,
            n=args.n,
            v_window=args.V,
            max_spread=args.max_spread,
            max_funding_abs_bps=args.max_funding_abs_bps,
        ),
        symbols=symbols,
    )

    order_placer = None
    if args.enable_trading:
        api_key = _read_secret(args.api_key_path)
        api_secret = _read_secret(args.api_secret_path)
        order_placer = OrderPlacer(api_key=api_key, api_secret=api_secret)
        risk_setup = order_placer.ensure_risk_setup(symbols=symbols, leverage=10, margin_type="ISOLATED")
        print(f"[RISK_SETUP] {risk_setup}")
        print("Live trading: ENABLED")
    else:
        print("Live trading: DISABLED")

    startup = client.rest_snapshot()
    client._seed_from_rest_snapshot(startup)
    client.ws.start()
    streams = client._build_combined_streams()
    client.ws.live_subscribe(streams, id=1, callback=client._on_ws_message)

    positions: Dict[str, PositionState] = {}
    cooldown_until_ms: Dict[str, int] = {}
    start = time.time()
    try:
        while (time.time() - start) < args.poll_time and not client._stop_event.is_set():
            ts_ms = _now_ms()

            with client._lock:
                symbol_rows = {}
                for sym in symbols:
                    symbol_rows[sym] = {
                        "bars": client.getBars(sym),
                        "bbo": client.getBBO(sym),
                        "funding": client.getFundingInfo(sym),
                        "trades_1s": client.getTrades(sym, lookback_seconds=1),
                        "l2": client.getL2(sym),
                    }
    
            if args.update_logs:
                client.logger.write_second(ts_ms, symbol_rows)

            for sym in symbols:
                snap = symbol_rows[sym]
                decision = strat.on_second(
                    symbol=sym,
                    bars_1m=snap.get("bars"),
                    bbo=snap.get("bbo"),
                    funding=snap.get("funding"),
                    now_ms=ts_ms,
                )

                if decision and "enter" in decision:
                    print(f"[SIGNAL] {sym} {decision}")
                if decision and ("ret_bps" in decision or "avg_base_vol" in decision):
                    print(
                        (
                            f"[BAR] {sym} ret_bps={decision.get('ret_bps')} "
                            f"rs_vol_bps_T={decision.get('rs_vol_bps')} "
                            f"bar_vol_1m={decision.get('bar_base_vol')} "
                            f"avg_vol_V={decision.get('avg_base_vol')} "
                            f"info={decision.get('info')}"
                        )
                    )

                if order_placer is None:
                    continue

                pos = positions.get(sym)
                if pos is not None:
                    live_qty = order_placer.get_position_abs_qty(sym)
                    if live_qty is not None and live_qty <= 0:
                        print(f"[POSITION] {sym} appears closed on exchange. Clearing local position state.")
                        order_placer.cancel_sibling_exit_orders(pos)
                        del positions[sym]
                        cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                        continue

                    exit_res = order_placer.maybe_exit(
                        pos=pos,
                        price_source=client,
                        c1_bps=0.0,
                        c2_bps=0.0,
                        margin_safety_multiple_min=args.margin_safety_multiple,
                        account_poll=True,
                    )
                    if exit_res is not None:
                        print(f"[EXIT] {sym} {exit_res}")
                        if exit_res.ok:
                            order_placer.cancel_sibling_exit_orders(pos)
                            del positions[sym]
                            cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                    continue

                if not decision or not decision.get("enter"):
                    continue

                # Enforce post-exit cooldown before new entries.
                if ts_ms < cooldown_until_ms.get(sym, 0):
                    remaining_s = (cooldown_until_ms[sym] - ts_ms) // 1000
                    print(f"[ENTRY_BLOCKED] {sym} cooldown active, remaining={remaining_s}s")
                    continue

                # If local state is empty but exchange still has position, do not re-enter/increase.
                live_qty = order_placer.get_position_abs_qty(sym)
                if live_qty is not None and live_qty > 0:
                    print(f"[ENTRY_BLOCKED] {sym} exchange position still open qty={live_qty}")
                    continue

                side = str(decision.get("side") or "")
                blockers = decision.get("blockers") or {}
                funding = snap.get("funding") or {}
                opening_loss_bps = float(max(0.0, blockers.get("opening_loss_bps") or 0.0))
                funding_bps = abs(float(funding.get("funding_rate") or 0.0) * 1e4)
                tp_bps = args.take_profit_mult * (args.taker_fee_bps + opening_loss_bps + (funding_bps / 8.0))
                sl_bps = args.stop_loss_bps if args.stop_loss_bps > 0 else tp_bps
                trailing_callback_rate = args.trailing_callback_rate if args.trailing_callback_rate > 0 else None

                entry_limit_price = order_placer.get_entry_limit_price(sym, side, client)
                if entry_limit_price is None:
                    print(f"[ENTRY_SKIP] {sym} no BBO to compute entry price/qty")
                    continue
                try:
                    order_qty = order_placer.compute_qty_for_notional(
                        symbol=sym,
                        entry_price=entry_limit_price,
                        order_notional_usd=args.order_notional,
                    )
                except Exception as e:
                    print(f"[ENTRY_SKIP] {sym} qty calc failed: {e}")
                    continue

                pos, entry_res = order_placer.entry(
                    symbol=sym,
                    side=side,
                    quantity=order_qty,
                    price_source=client,
                    take_profit_bps=tp_bps,
                    stop_loss_bps=sl_bps,
                    trailing_activation_frac=args.trailing_activation_frac,
                    trailing_callback_rate=trailing_callback_rate,
                )
                print(f"[ENTRY] {sym} {entry_res}")
                if pos is not None:
                    positions[sym] = pos
                    mark_px_now = (snap.get("funding") or {}).get("mark_px")
                    print(
                        (
                            f"[ENTRY_LEVELS] {sym} position={pos} "
                            f"taker_limit_price={entry_res.raw.get('taker_limit_price')} "
                            f"take_profit_price={entry_res.raw.get('take_profit_price')} "
                            f"stop_loss_mark_price={entry_res.raw.get('stop_loss_mark_price')} "
                            f"trailing_activation_price={entry_res.raw.get('trailing_activation_price')} "
                            f"trailing_callback_rate={entry_res.raw.get('trailing_callback_rate')} "
                            f"current_mark_price={mark_px_now}"
                        )
                    )

            client.n_poll_snapshots += 1
            time.sleep(client.poll_seconds)
    finally:
        client._stop_event.set()
        client.logger.close()
        client.graceful_shutdown(handshake_wait_seconds=0.8)

    print("done:", {"n_poll_snapshots": client.n_poll_snapshots})
    print(f"logs written to {args.log_dir}")
