from __future__ import annotations

import argparse
import time
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
from dotenv import load_dotenv

from client import AsterClient
from order import OrderPlacer, PositionState
from strategy import Strategy, StrategyConfig

load_dotenv()


def _to_bool(s: str) -> bool:
    return str(s).strip().lower() == "true"


def _read_secret(value_or_path: str | None) -> str:
    if not value_or_path:
        raise ValueError("Missing secret value/path in environment.")
    p = Path(value_or_path).expanduser()
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return str(value_or_path).strip()


def _now_ms() -> int:
    return int(time.time() * 1000)

def _parse_hhmm_utc(s: str) -> int:
    parts = str(s).strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Expected HH:MM, got: {s!r}")
    hh = int(parts[0])
    mm = int(parts[1])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError(f"Invalid HH:MM, got: {s!r}")
    return hh * 60 + mm


def _utc_minute_of_day(ts_ms: int) -> tuple[int, datetime]:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.hour * 60 + dt.minute, dt


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
    parser.add_argument("--order_notional", type=float, default=5.0)
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

    # Daily UTC schedule controls (used for maintenance windows/restarts).
    parser.add_argument("--entry_halt_utc", type=str, default="23:00")
    parser.add_argument("--force_exit_utc", type=str, default="23:50")

    args = parser.parse_args()
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

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    entry_halt_min = _parse_hhmm_utc(args.entry_halt_utc)
    force_exit_min = _parse_hhmm_utc(args.force_exit_utc)

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
        api_key = _read_secret(os.getenv("ORDER_API_KEY"))
        api_secret = _read_secret(os.getenv("ORDER_SECRET_KEY"))
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
    last_force_exit_attempt_ms: Dict[str, int] = {}
    daily_balance_day: Optional[str] = None
    daily_peak_balance: Optional[float] = None
    daily_last_balance: Optional[float] = None
    daily_drawdown_frac = 0.0
    daily_drawdown_blocked = False
    daily_balance_missing_warned = False
    start = time.time()
    try:
        while (time.time() - start) < args.poll_time and not client._stop_event.is_set():
            ts_ms = _now_ms()
            utc_minute, utc_dt = _utc_minute_of_day(ts_ms)
            if order_placer is not None:
                utc_day = utc_dt.date().isoformat()
                if utc_day != daily_balance_day:
                    daily_balance_day = utc_day
                    daily_peak_balance = None
                    daily_last_balance = None
                    daily_drawdown_frac = 0.0
                    daily_drawdown_blocked = False
                    daily_balance_missing_warned = False
                    print(f"[RISK_DAY_RESET] utc_day={utc_day}")

                balance_now = order_placer.get_total_margin_balance()
                if balance_now is not None and balance_now > 0:
                    daily_last_balance = balance_now
                    daily_balance_missing_warned = False
                    if daily_peak_balance is None or balance_now > daily_peak_balance:
                        daily_peak_balance = balance_now
                    if daily_peak_balance and daily_peak_balance > 0:
                        daily_drawdown_frac = max(0.0, (daily_peak_balance - balance_now) / daily_peak_balance)
                        if (
                            (not daily_drawdown_blocked)
                            and daily_drawdown_frac >= (args.daily_drawdown_blocker_pct / 100.0)
                        ):
                            daily_drawdown_blocked = True
                            print(
                                (
                                    f"[RISK_BLOCK] daily drawdown triggered "
                                    f"dd_pct={daily_drawdown_frac * 100.0:.3f} "
                                    f"threshold_pct={args.daily_drawdown_blocker_pct:.3f} "
                                    f"peak_balance={daily_peak_balance:.6f} "
                                    f"current_balance={balance_now:.6f} "
                                    f"utc_day={utc_day}"
                                )
                            )
                elif not daily_balance_missing_warned:
                    daily_balance_missing_warned = True
                    print("[RISK_WARN] could not read totalMarginBalance; daily drawdown blocker is inactive until balance is available.")

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
                    if daily_drawdown_blocked:
                        last_attempt = last_force_exit_attempt_ms.get(sym, 0)
                        if ts_ms - last_attempt >= 10_000:
                            last_force_exit_attempt_ms[sym] = ts_ms
                            exit_res = order_placer.close_position(
                                pos=pos,
                                price_source=client,
                                reason="DAILY_DRAWDOWN_BLOCK",
                                notes=(
                                    f"dd_pct={daily_drawdown_frac * 100.0:.4f} >= "
                                    f"{args.daily_drawdown_blocker_pct:.4f}"
                                ),
                            )
                            print(f"[EXIT] {sym} {exit_res}")
                            if exit_res.ok:
                                order_placer.cancel_sibling_exit_orders(pos)
                                del positions[sym]
                                cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                        continue

                    # Daily maintenance window: force-close any open position by cutoff.
                    if utc_minute >= force_exit_min:
                        last_attempt = last_force_exit_attempt_ms.get(sym, 0)
                        if ts_ms - last_attempt >= 10_000:
                            last_force_exit_attempt_ms[sym] = ts_ms
                            exit_res = order_placer.close_position(
                                pos=pos,
                                price_source=client,
                                reason="DAILY_CUTOFF",
                                notes=f"utc={utc_dt.isoformat()} >= {args.force_exit_utc}",
                            )
                            print(f"[EXIT] {sym} {exit_res}")
                            if exit_res.ok:
                                order_placer.cancel_sibling_exit_orders(pos)
                                del positions[sym]
                                cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                        continue

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

                # If local state is empty but exchange still has position, ensure we flatten
                # before the daily restart window.
                if daily_drawdown_blocked:
                    live_amt = order_placer.get_position_amt(sym)
                    if live_amt is not None and abs(live_amt) > 0:
                        last_attempt = last_force_exit_attempt_ms.get(sym, 0)
                        if ts_ms - last_attempt >= 10_000:
                            last_force_exit_attempt_ms[sym] = ts_ms
                            temp_pos = PositionState(
                                symbol=sym,
                                side=("BUY" if live_amt > 0 else "SELL"),
                                qty=abs(live_amt),
                                entry_vwap_px=0.0,
                                opened_time_ms=ts_ms,
                            )
                            exit_res = order_placer.close_position(
                                pos=temp_pos,
                                price_source=client,
                                reason="DAILY_DRAWDOWN_BLOCK",
                                notes=(
                                    f"dd_pct={daily_drawdown_frac * 100.0:.4f} >= "
                                    f"{args.daily_drawdown_blocker_pct:.4f}"
                                ),
                            )
                            print(f"[EXIT] {sym} {exit_res}")
                            if exit_res.ok:
                                cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                        continue
                    print(
                        (
                            f"[ENTRY_BLOCKED] {sym} daily_drawdown_blocker active "
                            f"dd_pct={daily_drawdown_frac * 100.0:.4f} "
                            f"threshold_pct={args.daily_drawdown_blocker_pct:.4f}"
                        )
                    )
                    continue

                if utc_minute >= force_exit_min:
                    live_amt = order_placer.get_position_amt(sym)
                    if live_amt is not None and abs(live_amt) > 0:
                        last_attempt = last_force_exit_attempt_ms.get(sym, 0)
                        if ts_ms - last_attempt >= 10_000:
                            last_force_exit_attempt_ms[sym] = ts_ms
                            temp_pos = PositionState(
                                symbol=sym,
                                side=("BUY" if live_amt > 0 else "SELL"),
                                qty=abs(live_amt),
                                entry_vwap_px=0.0,
                                opened_time_ms=ts_ms,
                            )
                            exit_res = order_placer.close_position(
                                pos=temp_pos,
                                price_source=client,
                                reason="DAILY_CUTOFF",
                                notes=f"utc={utc_dt.isoformat()} >= {args.force_exit_utc}",
                            )
                            print(f"[EXIT] {sym} {exit_res}")
                            if exit_res.ok:
                                cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                        continue

                if not decision or not decision.get("enter"):
                    continue

                # Daily maintenance window: stop new entries before restart window.
                if utc_minute >= entry_halt_min:
                    print(
                        f"[ENTRY_BLOCKED] {sym} entry_halt_utc={args.entry_halt_utc} now_utc={utc_dt.isoformat()}"
                    )
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
                be_floor_bps = 2.0 * args.taker_fee_bps + opening_loss_bps + (funding_bps / 8.0)
                activation_auto_bps = be_floor_bps + max(0.0, args.trailing_activation_buffer_bps)
                activation_bps = max(args.trailing_activation_bps, activation_auto_bps)
                tp_bps = max(args.take_profit_bps, activation_bps + max(0.0, args.min_take_profit_gap_bps))
                sl_bps = args.stop_loss_bps
                trailing_callback_rate = args.trailing_callback_bps / 1e4

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
                    trailing_activation_bps=activation_bps,
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
                            f"be_floor_bps={be_floor_bps:.4f} "
                            f"activation_bps={activation_bps:.4f} "
                            f"take_profit_bps={tp_bps:.4f} "
                            f"stop_loss_bps={sl_bps:.4f} "
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
