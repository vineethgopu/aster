from __future__ import annotations

import argparse
import csv
import os
import smtplib
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, Optional, Set

from dotenv import load_dotenv

from client import AsterClient
from order import OrderPlacer, PositionState
from strategy import Strategy, StrategyConfig

load_dotenv()


TRADE_LIFECYCLE_FIELDS = [
    "exit_fill_time_ms",
    "exit_fill_time_utc",
    "symbol",
    "entry_order_id",
    "exit_order_id",
    "exit_reason",
    "exit_send_time_ms",
    "exit_send_time_utc",
    "entry_send_time_ms",
    "entry_send_time_utc",
    "entry_fill_time_ms",
    "entry_fill_time_utc",
    "entry_fill_price",
    "fill_quantity",
    "fill_notional",
    "exit_fill_price",
    "raw_return_pct",
    "position_return_pct",
    "order_lifetime_market_volume_quantity",
    "order_lifetime_market_volume_notional",
    "order_lifetime_open",
    "order_lifetime_high",
    "order_lifetime_low",
    "order_lifetime_close",
    "order_lifetime_vwap",
    "order_duration_s",
    "entry_mark_price",
    "exit_mark_price",
    "mark_price_change_bps",
    "fees_notional",
    "gross_pnl_notional",
    "total_pnl_notional",
]


def _to_bool(s: str) -> bool:
    return str(s).strip().lower() == "true"


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _read_secret(value_or_path: str | None) -> str:
    if not value_or_path:
        raise ValueError("Missing secret value/path in environment.")
    p = Path(value_or_path).expanduser()
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return str(value_or_path).strip()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_utc_ms(ts_ms: Optional[int]) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")


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


def _extract_update_time_ms(raw_query: Any) -> Optional[int]:
    if not isinstance(raw_query, dict):
        return None
    d = raw_query.get("data") if isinstance(raw_query.get("data"), dict) else raw_query
    t = _safe_float(d.get("updateTime")) if isinstance(d, dict) else None
    return int(t) if t is not None else None


def _resolve_email_smtp_pass() -> str:
    secret_dir = Path(os.getenv("ASTER_SECRET_DIR", "/opt/aster/.secrets"))
    p = secret_dir / "email_smtp_pass"
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return ""


def _send_trade_alert_email(subject: str, body: str) -> None:
    smtp_host = os.getenv("ASTER_EMAIL_SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("ASTER_EMAIL_SMTP_PORT", "587"))
    smtp_user = os.getenv("ASTER_EMAIL_SMTP_USER", "").strip()
    smtp_pass = _resolve_email_smtp_pass()
    recipients = [x.strip() for x in os.getenv("ASTER_EMAIL_TO_PROD", "").split(",") if x.strip()]
    if not (smtp_host and smtp_user and smtp_pass and recipients):
        print("[TRADE_EMAIL] SMTP config/recipients missing; skipping trade alert email.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


def _append_trade_lifecycle_row(log_dir: str, row: Dict[str, Any]) -> None:
    exit_ts_ms = int(row.get("exit_fill_time_ms") or _now_ms())
    date_str = datetime.fromtimestamp(exit_ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")
    path = Path(log_dir) / f"orders_{date_str}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = (not path.exists()) or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_LIFECYCLE_FIELDS)
        if needs_header:
            w.writeheader()
        w.writerow({k: row.get(k) for k in TRADE_LIFECYCLE_FIELDS})


def _map_exit_reason(reason: str) -> str:
    r = str(reason or "").upper()
    if r in {"TP", "SL", "TSL", "MARGIN"}:
        return r
    if "MARGIN" in r:
        return "MARGIN"
    if "TAKE_PROFIT" in r:
        return "TP"
    if "TRAIL" in r:
        return "TSL"
    if "STOP" in r:
        return "SL"
    return r or "UNKNOWN"


def _update_trade_tracker(tracker: Dict[str, Any], snap: Dict[str, Any]) -> None:
    funding = snap.get("funding") or {}
    mark_px = _safe_float(funding.get("mark_px"))
    if mark_px is not None and mark_px > 0:
        tracker["exit_mark_price"] = mark_px

    trades = snap.get("trades_1s") or []
    seen: Set[int] = tracker["seen_trade_ids"]
    for t in trades:
        if not isinstance(t, dict):
            continue
        agg_id = t.get("agg_id")
        if agg_id is None:
            continue
        try:
            agg_id_int = int(agg_id)
        except Exception:
            continue
        if agg_id_int in seen:
            continue
        seen.add(agg_id_int)
        px = _safe_float(t.get("price"))
        qty = _safe_float(t.get("qty"))
        if px is None or qty is None or qty <= 0:
            continue

        tracker["order_lifetime_market_volume_quantity"] += qty
        tracker["order_lifetime_market_volume_notional"] += px * qty

        if tracker["order_lifetime_open"] is None:
            tracker["order_lifetime_open"] = px
            tracker["order_lifetime_high"] = px
            tracker["order_lifetime_low"] = px
        tracker["order_lifetime_close"] = px
        tracker["order_lifetime_high"] = max(float(tracker["order_lifetime_high"]), px)
        tracker["order_lifetime_low"] = min(float(tracker["order_lifetime_low"]), px)


def _finalize_trade(
    *,
    symbol: str,
    pos: PositionState,
    tracker: Dict[str, Any],
    exit_reason: str,
    exit_order_id: Optional[int],
    exit_send_time_ms_hint: Optional[int],
    exit_fill_price_hint: Optional[float],
    exit_fill_time_ms_hint: Optional[int],
    order_placer: OrderPlacer,
    log_dir: str,
    send_trade_alert_email: bool,
) -> None:
    now_ms = _now_ms()
    entry_order_id = tracker.get("entry_order_id")
    entry_stats = order_placer.get_order_trade_stats(
        symbol=symbol,
        order_id=entry_order_id,
        start_time_ms=(tracker.get("entry_send_time_ms") or now_ms) - 15 * 60_000,
        end_time_ms=now_ms + 60_000,
    )
    exit_stats = order_placer.get_order_trade_stats(
        symbol=symbol,
        order_id=exit_order_id,
        start_time_ms=(tracker.get("entry_send_time_ms") or now_ms) - 15 * 60_000,
        end_time_ms=now_ms + 60_000,
    )

    entry_fill_price = _safe_float(entry_stats.get("avg_price")) or _safe_float(tracker.get("entry_fill_price")) or 0.0
    fill_quantity = _safe_float(entry_stats.get("executed_qty")) or _safe_float(tracker.get("fill_quantity")) or float(pos.qty)
    fill_notional = _safe_float(entry_stats.get("notional")) or (entry_fill_price * fill_quantity)
    entry_fill_time_ms = (
        _safe_float(entry_stats.get("exec_time_ms"))
        or _safe_float(tracker.get("entry_fill_time_ms"))
        or _safe_float(tracker.get("entry_send_time_ms"))
        or now_ms
    )
    entry_fill_time_ms = int(entry_fill_time_ms)

    exit_fill_price = _safe_float(exit_stats.get("avg_price")) or _safe_float(exit_fill_price_hint) or entry_fill_price
    exit_fill_time_ms = (
        _safe_float(exit_stats.get("exec_time_ms"))
        or _safe_float(exit_fill_time_ms_hint)
        or now_ms
    )
    exit_fill_time_ms = int(exit_fill_time_ms)
    exit_send_time_ms = int(exit_send_time_ms_hint) if exit_send_time_ms_hint is not None else exit_fill_time_ms

    fees_notional = float(_safe_float(entry_stats.get("fee")) or 0.0) + float(_safe_float(exit_stats.get("fee")) or 0.0)

    raw_return = ((exit_fill_price - entry_fill_price) / entry_fill_price) if entry_fill_price > 0 else 0.0
    signed_return = raw_return if pos.is_long else -raw_return
    gross_pnl_notional = fill_notional * signed_return
    total_pnl_notional = gross_pnl_notional - fees_notional

    market_qty = float(tracker.get("order_lifetime_market_volume_quantity", 0.0))
    market_notional = float(tracker.get("order_lifetime_market_volume_notional", 0.0))
    lifetime_vwap = (market_notional / market_qty) if market_qty > 0 else None
    o = _safe_float(tracker.get("order_lifetime_open"))
    h = _safe_float(tracker.get("order_lifetime_high"))
    l = _safe_float(tracker.get("order_lifetime_low"))
    c = _safe_float(tracker.get("order_lifetime_close"))
    if o is None:
        o = entry_fill_price
        h = max(entry_fill_price, exit_fill_price)
        l = min(entry_fill_price, exit_fill_price)
        c = exit_fill_price

    mark_entry = _safe_float(tracker.get("entry_mark_price"))
    mark_exit = _safe_float(tracker.get("exit_mark_price"))
    mark_change_bps = None
    if mark_entry is not None and mark_exit is not None and mark_entry != 0:
        mark_change_bps = 1e4 * (mark_exit / mark_entry - 1.0)

    duration_s = max(0.0, (exit_fill_time_ms - entry_fill_time_ms) / 1000.0)
    reason = _map_exit_reason(exit_reason)

    entry_send_time_ms_val = _safe_float(tracker.get("entry_send_time_ms"))
    entry_send_time_ms_int = int(entry_send_time_ms_val) if entry_send_time_ms_val is not None else None

    row = {
        "exit_fill_time_ms": exit_fill_time_ms,
        "exit_fill_time_utc": _fmt_utc_ms(exit_fill_time_ms),
        "symbol": symbol,
        "entry_order_id": entry_order_id,
        "exit_order_id": exit_order_id,
        "exit_reason": reason,
        "exit_send_time_ms": exit_send_time_ms,
        "exit_send_time_utc": _fmt_utc_ms(exit_send_time_ms),
        "entry_send_time_ms": entry_send_time_ms_int,
        "entry_send_time_utc": _fmt_utc_ms(entry_send_time_ms_int),
        "entry_fill_time_ms": entry_fill_time_ms,
        "entry_fill_time_utc": _fmt_utc_ms(entry_fill_time_ms),
        "entry_fill_price": entry_fill_price,
        "fill_quantity": fill_quantity,
        "fill_notional": fill_notional,
        "exit_fill_price": exit_fill_price,
        "raw_return_pct": 100.0 * raw_return,
        "position_return_pct": 100.0 * signed_return,
        "order_lifetime_market_volume_quantity": market_qty,
        "order_lifetime_market_volume_notional": market_notional,
        "order_lifetime_open": o,
        "order_lifetime_high": h,
        "order_lifetime_low": l,
        "order_lifetime_close": c,
        "order_lifetime_vwap": lifetime_vwap,
        "order_duration_s": duration_s,
        "entry_mark_price": mark_entry,
        "exit_mark_price": mark_exit,
        "mark_price_change_bps": mark_change_bps,
        "fees_notional": fees_notional,
        "gross_pnl_notional": gross_pnl_notional,
        "total_pnl_notional": total_pnl_notional,
    }
    _append_trade_lifecycle_row(log_dir=log_dir, row=row)

    if send_trade_alert_email:
        body = (
            "Aster Trade Alert\n\n"
            f"symbol: {symbol}\n"
            f"entry_id: {entry_order_id}\n"
            f"exit_id: {exit_order_id}\n"
            f"exit_reason: {reason}\n"
            f"exit_send_time_utc: {row['exit_send_time_utc']}\n"
            f"entry_send_time_utc: {row['entry_send_time_utc']}\n"
            f"entry_fill_time_utc: {row['entry_fill_time_utc']}\n"
            f"entry_fill_price: {entry_fill_price}\n"
            f"fill_quantity: {fill_quantity}\n"
            f"fill_notional: {fill_notional}\n"
            f"exit_fill_price: {exit_fill_price}\n"
            f"(exit-entry)/entry_pct: {row['raw_return_pct']}\n"
            f"order_lifetime_market_volume_quantity: {market_qty}\n"
            f"order_lifetime_market_volume_notional: {market_notional}\n"
            f"lifetime_ohlc: O={o}, H={h}, L={l}, C={c}\n"
            f"order_lifetime_vwap: {lifetime_vwap}\n"
            f"order_duration_s: {duration_s}\n"
            f"entry_mark_price: {mark_entry}\n"
            f"exit_mark_price: {mark_exit}\n"
            f"mark_price_change_bps: {mark_change_bps}\n"
            f"fees_notional: {fees_notional}\n"
            f"gross_pnl_notional: {gross_pnl_notional}\n"
            f"total_pnl_notional: {total_pnl_notional}\n"
        )
        subject = f"Aster Trade Closed {symbol} {reason}"
        try:
            _send_trade_alert_email(subject=subject, body=body)
        except Exception as e:
            print(f"[TRADE_EMAIL] send failed: {e}")


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
    if args.risk_pct <= 0:
        raise ValueError("--risk_pct must be > 0")
    if args.target_leverage <= 0:
        raise ValueError("--target_leverage must be > 0")
    if args.order_notional is not None and args.order_notional <= 0:
        raise ValueError("--order_notional must be > 0 when provided")

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
        risk_setup = order_placer.ensure_risk_setup(
            symbols=symbols,
            leverage=args.target_leverage,
            margin_type="ISOLATED",
        )
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
    trade_trackers: Dict[str, Dict[str, Any]] = {}
    cooldown_until_ms: Dict[str, int] = {}
    last_force_exit_attempt_ms: Dict[str, int] = {}
    daily_balance_day: Optional[str] = None
    daily_start_balance: Optional[float] = None
    daily_peak_balance: Optional[float] = None
    daily_last_balance: Optional[float] = None
    daily_drawdown_frac = 0.0
    daily_drawdown_blocked = False
    daily_balance_missing_warned = False
    effective_order_notional = args.order_notional
    start = time.time()

    try:
        while (time.time() - start) < args.poll_time and not client._stop_event.is_set():
            ts_ms = _now_ms()
            utc_minute, utc_dt = _utc_minute_of_day(ts_ms)
            if order_placer is not None:
                utc_day = utc_dt.date().isoformat()
                if utc_day != daily_balance_day:
                    daily_balance_day = utc_day
                    daily_start_balance = None
                    daily_peak_balance = None
                    daily_last_balance = None
                    daily_drawdown_frac = 0.0
                    daily_drawdown_blocked = False
                    daily_balance_missing_warned = False
                    effective_order_notional = args.order_notional
                    print(f"[RISK_DAY_RESET] utc_day={utc_day}")

                balance_now = order_placer.get_total_margin_balance()
                if balance_now is not None and balance_now > 0:
                    daily_last_balance = balance_now
                    daily_balance_missing_warned = False
                    if daily_start_balance is None:
                        daily_start_balance = balance_now
                        if args.order_notional is None:
                            effective_order_notional = daily_start_balance * (args.risk_pct / 100.0) * args.target_leverage
                            print(
                                f"[NOTIONAL_DEFAULT] start_balance={daily_start_balance:.6f} "
                                f"risk_pct={args.risk_pct:.4f}% leverage={args.target_leverage} "
                                f"order_notional={effective_order_notional:.6f}"
                            )
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
                    print("[RISK_WARN] could not read totalMarginBalance; daily drawdown blocker/default notional unavailable until balance is available.")

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
                    tracker = trade_trackers.get(sym)
                    if tracker is not None:
                        _update_trade_tracker(tracker, snap)

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
                                if tracker is not None:
                                    _finalize_trade(
                                        symbol=sym,
                                        pos=pos,
                                        tracker=tracker,
                                        exit_reason=exit_res.reason,
                                        exit_order_id=exit_res.close_order_id,
                                        exit_send_time_ms_hint=ts_ms,
                                        exit_fill_price_hint=exit_res.close_vwap_px,
                                        exit_fill_time_ms_hint=_extract_update_time_ms(exit_res.raw.get("close_query") if isinstance(exit_res.raw, dict) else None),
                                        order_placer=order_placer,
                                        log_dir=args.log_dir,
                                        send_trade_alert_email=args.trade_alert_email,
                                    )
                                order_placer.cancel_sibling_exit_orders(pos)
                                del positions[sym]
                                trade_trackers.pop(sym, None)
                                cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                        continue

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
                                if tracker is not None:
                                    _finalize_trade(
                                        symbol=sym,
                                        pos=pos,
                                        tracker=tracker,
                                        exit_reason=exit_res.reason,
                                        exit_order_id=exit_res.close_order_id,
                                        exit_send_time_ms_hint=ts_ms,
                                        exit_fill_price_hint=exit_res.close_vwap_px,
                                        exit_fill_time_ms_hint=_extract_update_time_ms(exit_res.raw.get("close_query") if isinstance(exit_res.raw, dict) else None),
                                        order_placer=order_placer,
                                        log_dir=args.log_dir,
                                        send_trade_alert_email=args.trade_alert_email,
                                    )
                                order_placer.cancel_sibling_exit_orders(pos)
                                del positions[sym]
                                trade_trackers.pop(sym, None)
                                cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                        continue

                    live_qty = order_placer.get_position_abs_qty(sym)
                    if live_qty is not None and live_qty <= 0:
                        detect = order_placer.detect_filled_exit_order(pos)
                        print(f"[POSITION] {sym} appears closed on exchange. detected_exit={detect}")
                        if tracker is not None:
                            _finalize_trade(
                                symbol=sym,
                                pos=pos,
                                tracker=tracker,
                                exit_reason=detect.get("reason") or "UNKNOWN",
                                exit_order_id=detect.get("order_id"),
                                exit_send_time_ms_hint=None,
                                exit_fill_price_hint=_safe_float(detect.get("avg_price")),
                                exit_fill_time_ms_hint=(int(detect.get("update_time_ms")) if detect.get("update_time_ms") is not None else None),
                                order_placer=order_placer,
                                log_dir=args.log_dir,
                                send_trade_alert_email=args.trade_alert_email,
                            )
                        order_placer.cancel_sibling_exit_orders(pos)
                        del positions[sym]
                        trade_trackers.pop(sym, None)
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
                            if tracker is not None:
                                _finalize_trade(
                                    symbol=sym,
                                    pos=pos,
                                    tracker=tracker,
                                    exit_reason=exit_res.reason,
                                    exit_order_id=exit_res.close_order_id,
                                    exit_send_time_ms_hint=ts_ms,
                                    exit_fill_price_hint=exit_res.close_vwap_px,
                                    exit_fill_time_ms_hint=_extract_update_time_ms(exit_res.raw.get("close_query") if isinstance(exit_res.raw, dict) else None),
                                    order_placer=order_placer,
                                    log_dir=args.log_dir,
                                    send_trade_alert_email=args.trade_alert_email,
                                )
                            order_placer.cancel_sibling_exit_orders(pos)
                            del positions[sym]
                            trade_trackers.pop(sym, None)
                            cooldown_until_ms[sym] = ts_ms + args.reentry_cooldown_min * 60_000
                    continue

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

                if utc_minute >= entry_halt_min:
                    print(f"[ENTRY_BLOCKED] {sym} entry_halt_utc={args.entry_halt_utc} now_utc={utc_dt.isoformat()}")
                    continue

                if ts_ms < cooldown_until_ms.get(sym, 0):
                    remaining_s = (cooldown_until_ms[sym] - ts_ms) // 1000
                    print(f"[ENTRY_BLOCKED] {sym} cooldown active, remaining={remaining_s}s")
                    continue

                live_qty = order_placer.get_position_abs_qty(sym)
                if live_qty is not None and live_qty > 0:
                    print(f"[ENTRY_BLOCKED] {sym} exchange position still open qty={live_qty}")
                    continue

                if effective_order_notional is None or effective_order_notional <= 0:
                    print(f"[ENTRY_BLOCKED] {sym} order_notional unavailable (awaiting balance/default calc).")
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
                        order_notional_usd=effective_order_notional,
                    )
                except Exception as e:
                    print(f"[ENTRY_SKIP] {sym} qty calc failed: {e}")
                    continue

                entry_send_ms = ts_ms
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
                    mark_px_now = _safe_float((snap.get("funding") or {}).get("mark_px"))
                    entry_fill_time_ms = _extract_update_time_ms(entry_res.raw.get("taker_query") if isinstance(entry_res.raw, dict) else None) or _now_ms()
                    trade_trackers[sym] = {
                        "entry_order_id": pos.taker_order_id,
                        "entry_send_time_ms": entry_send_ms,
                        "entry_fill_time_ms": entry_fill_time_ms,
                        "entry_fill_price": pos.entry_vwap_px,
                        "fill_quantity": pos.qty,
                        "entry_mark_price": mark_px_now,
                        "exit_mark_price": mark_px_now,
                        "order_lifetime_market_volume_quantity": 0.0,
                        "order_lifetime_market_volume_notional": 0.0,
                        "order_lifetime_open": None,
                        "order_lifetime_high": None,
                        "order_lifetime_low": None,
                        "order_lifetime_close": None,
                        "seen_trade_ids": set(),
                    }
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
                            f"current_mark_price={mark_px_now} "
                            f"order_notional={effective_order_notional:.6f}"
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
