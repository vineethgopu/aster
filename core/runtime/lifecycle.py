from __future__ import annotations

import csv
import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, Optional

from exchange.client import AsterClient
from execution.order import OrderPlacer, PositionState
from .utils import _fmt_utc_ms, _now_ms, _resolve_email_smtp_pass, _safe_float


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


def new_trade_tracker(
    *,
    symbol: str,
    side: str,
    entry_send_time_ms: int,
    entry_mark_price: Optional[float],
    client: AsterClient,
) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "entry_side": str(side or "").upper(),
        "entry_order_id": None,
        "entry_send_time_ms": int(entry_send_time_ms),
        "entry_fill_time_ms": None,
        "entry_fill_price": None,
        "fill_quantity": 0.0,
        "entry_mark_price": entry_mark_price,
        "exit_mark_price": entry_mark_price,
        "order_lifetime_market_volume_quantity": 0.0,
        "order_lifetime_market_volume_notional": 0.0,
        "order_lifetime_open": None,
        "order_lifetime_high": None,
        "order_lifetime_low": None,
        "order_lifetime_close": None,
        "private_event_seq": int(client.get_latest_private_event_seq(symbol)),
        "market_event_seq": int(client.get_latest_agg_trade_event_seq(symbol)),
        "last_position_amt": None,
        "last_position_event_time_ms": None,
        "entry_order_update": None,
        "entry_order_status": "",
        "detected_exit": None,
        "seen_exit_order_ids": set(),
        "warned_private_truncation": False,
        "warned_market_truncation": False,
        "finalized": False,
    }


def record_tracker_fill(
    tracker: Dict[str, Any],
    *,
    fill_qty: Optional[float],
    fill_price: Optional[float],
    fill_time_ms: Optional[int],
) -> None:
    qty = _safe_float(fill_qty)
    px = _safe_float(fill_price)
    tm = _safe_float(fill_time_ms)
    prev_qty = _safe_float(tracker.get("fill_quantity")) or 0.0

    if qty is not None and qty > 0:
        tracker["fill_quantity"] = max(prev_qty, float(qty))
    if px is not None and px > 0:
        tracker["entry_fill_price"] = float(px)
    if tm is not None and tm > 0:
        tm_int = int(tm)
        current = _safe_float(tracker.get("entry_fill_time_ms"))
        if current is None or current <= 0 or tm_int < int(current):
            tracker["entry_fill_time_ms"] = tm_int


def _apply_trade_market_event(tracker: Dict[str, Any], trade: Dict[str, Any]) -> None:
    px = _safe_float(trade.get("price"))
    qty = _safe_float(trade.get("qty"))
    if px is None or qty is None or qty <= 0:
        return

    tracker["order_lifetime_market_volume_quantity"] += qty
    tracker["order_lifetime_market_volume_notional"] += px * qty

    if tracker["order_lifetime_open"] is None:
        tracker["order_lifetime_open"] = px
        tracker["order_lifetime_high"] = px
        tracker["order_lifetime_low"] = px
    tracker["order_lifetime_close"] = px
    tracker["order_lifetime_high"] = max(float(tracker["order_lifetime_high"]), px)
    tracker["order_lifetime_low"] = min(float(tracker["order_lifetime_low"]), px)


def drain_trade_tracker_events(
    *,
    client: AsterClient,
    symbol: str,
    tracker: Dict[str, Any],
    exit_order_ids_by_reason: Optional[Dict[str, Optional[int]]] = None,
    mark_px: Optional[float] = None,
) -> None:
    if tracker is None:
        return

    if mark_px is not None and mark_px > 0:
        tracker["exit_mark_price"] = mark_px

    entry_send_time_ms = int(tracker.get("entry_send_time_ms") or 0)
    market_drain = client.drain_agg_trade_events(
        symbol=symbol,
        since_seq=int(tracker.get("market_event_seq") or 0),
    )
    tracker["market_event_seq"] = int(market_drain.get("next_seq") or tracker.get("market_event_seq") or 0)
    if market_drain.get("truncated") and not tracker.get("warned_market_truncation"):
        tracker["warned_market_truncation"] = True
        print(f"[TRACKER_WARN] {symbol} agg-trade event buffer truncated; market lifetime stats may be incomplete.")
    for trade in market_drain.get("events") or []:
        trade_time_ms = _safe_float(trade.get("trade_time_ms"))
        if trade_time_ms is not None and trade_time_ms < entry_send_time_ms:
            continue
        _apply_trade_market_event(tracker, trade)

    private_drain = client.drain_private_events(
        symbol=symbol,
        since_seq=int(tracker.get("private_event_seq") or 0),
    )
    tracker["private_event_seq"] = int(private_drain.get("next_seq") or tracker.get("private_event_seq") or 0)
    if private_drain.get("truncated") and not tracker.get("warned_private_truncation"):
        tracker["warned_private_truncation"] = True
        print(f"[TRACKER_WARN] {symbol} private event buffer truncated; order lifecycle state may need REST fallback.")

    exit_reason_by_order_id: Dict[int, str] = {}
    for reason, oid in (exit_order_ids_by_reason or {}).items():
        if oid is None:
            continue
        exit_reason_by_order_id[int(oid)] = str(reason)
    entry_order_id = tracker.get("entry_order_id")
    for event in private_drain.get("events") or []:
        event_type = str(event.get("event_type") or "").upper()
        event_time_ms = _safe_float(event.get("event_time_ms"))

        if event_type == "ACCOUNT_POSITION":
            if event_time_ms is not None and event_time_ms < entry_send_time_ms:
                continue
            tracker["last_position_amt"] = _safe_float(event.get("position_amt"))
            tracker["last_position_event_time_ms"] = int(event_time_ms) if event_time_ms is not None else None
            continue

        if event_type != "ORDER_TRADE_UPDATE":
            continue

        order_update = event.get("order_update") if isinstance(event.get("order_update"), dict) else {}
        order_id_val = order_update.get("order_id")
        try:
            order_id = int(order_id_val)
        except Exception:
            continue

        status = str(order_update.get("status") or "").upper()
        cum_filled_qty = _safe_float(order_update.get("cum_filled_qty"))
        avg_price = _safe_float(order_update.get("avg_price")) or _safe_float(order_update.get("last_price"))
        fill_time_ms = _safe_float(order_update.get("transaction_time_ms")) or event_time_ms

        if entry_order_id is not None and order_id == int(entry_order_id):
            tracker["entry_order_update"] = order_update
            tracker["entry_order_status"] = status
            record_tracker_fill(
                tracker,
                fill_qty=cum_filled_qty,
                fill_price=avg_price,
                fill_time_ms=(int(fill_time_ms) if fill_time_ms is not None else None),
            )
            continue

        if order_id not in exit_reason_by_order_id:
            continue
        if status != "FILLED" or (cum_filled_qty or 0.0) <= 0:
            continue

        candidate = {
            "reason": exit_reason_by_order_id.get(order_id) or "UNKNOWN",
            "order_id": order_id,
            "filled_qty": float(cum_filled_qty or 0.0),
            "avg_price": avg_price,
            "update_time_ms": int(fill_time_ms) if fill_time_ms is not None else (int(event_time_ms) if event_time_ms is not None else None),
            "status": status,
            "raw": order_update,
        }
        current = tracker.get("detected_exit")
        current_tm = _safe_float((current or {}).get("update_time_ms")) if isinstance(current, dict) else None
        candidate_tm = _safe_float(candidate.get("update_time_ms"))
        if current is None or current_tm is None or (candidate_tm is not None and candidate_tm < current_tm):
            tracker["detected_exit"] = candidate


def resolve_tracker_position_state(
    *,
    account_state: Dict[str, Any],
    tracker: Optional[Dict[str, Any]],
) -> tuple[Optional[float], Optional[float], bool, Optional[str]]:
    ws_pos_amt = _safe_float(account_state.get("position_amt"))
    ws_last_update_ms = _safe_float(account_state.get("last_update_ms"))

    tracker_pos_amt = None
    tracker_pos_update_ms = None
    if tracker is not None:
        tracker_pos_amt = _safe_float(tracker.get("last_position_amt"))
        tracker_pos_update_ms = _safe_float(tracker.get("last_position_event_time_ms"))

    pos_amt = ws_pos_amt
    if tracker_pos_update_ms is not None and (ws_last_update_ms is None or tracker_pos_update_ms >= ws_last_update_ms):
        pos_amt = tracker_pos_amt

    pos_qty = abs(float(pos_amt)) if pos_amt is not None else None
    has_position = bool(pos_qty is not None and pos_qty > 0)
    pos_side = "BUY" if (pos_amt is not None and pos_amt > 0) else ("SELL" if (pos_amt is not None and pos_amt < 0) else None)
    return pos_amt, pos_qty, has_position, pos_side


def finalize_and_close_position(
    *,
    symbol: str,
    pos: PositionState,
    tracker: Optional[Dict[str, Any]],
    exit_reason: str,
    exit_order_id: Optional[int],
    exit_send_time_ms_hint: Optional[int],
    exit_fill_price_hint: Optional[float],
    exit_fill_time_ms_hint: Optional[int],
    order_placer: OrderPlacer,
    log_dir: str,
    send_trade_alert_email: bool,
    positions: Dict[str, PositionState],
    trade_trackers: Dict[str, Dict[str, Any]],
    cooldown_until_ms: Dict[str, int],
    reentry_cooldown_min: int,
    now_ms: int,
) -> None:
    if tracker is not None and not bool(tracker.get("finalized")):
        finalize_trade(
            symbol=symbol,
            pos=pos,
            tracker=tracker,
            exit_reason=exit_reason,
            exit_order_id=exit_order_id,
            exit_send_time_ms_hint=exit_send_time_ms_hint,
            exit_fill_price_hint=exit_fill_price_hint,
            exit_fill_time_ms_hint=exit_fill_time_ms_hint,
            order_placer=order_placer,
            log_dir=log_dir,
            send_trade_alert_email=send_trade_alert_email,
        )
        tracker["finalized"] = True
    order_placer.cancel_sibling_exit_orders(pos)
    positions.pop(symbol, None)
    trade_trackers.pop(symbol, None)
    cooldown_until_ms[symbol] = now_ms + reentry_cooldown_min * 60_000


def finalize_trade(
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
