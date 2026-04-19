from __future__ import annotations

import time
from typing import Any, Dict, Optional

from exchange.client import AsterClient
from execution.order import OrderPlacer, PositionState
from .utils import _extract_update_time_ms, _now_ms, _safe_float, _utc_minute_of_day
from .lifecycle import (
    drain_trade_tracker_events,
    finalize_and_close_position,
    new_trade_tracker,
    record_tracker_fill,
    resolve_tracker_position_state,
)


def run_live_runtime(
    *,
    args: Any,
    client: AsterClient,
    order_placer: Optional[OrderPlacer],
    symbols: list[str],
    symbol_params: Dict[str, Dict[str, Any]],
    strat_by_symbol: Dict[str, Any],
    entry_halt_min: int,
    force_exit_min: int,
) -> None:
    startup = client.rest_snapshot()
    client._seed_from_rest_snapshot(startup)
    client.ws.start()
    streams = client._build_combined_streams()
    client.ws.live_subscribe(streams, id=1, callback=client._on_ws_message)
    if order_placer is not None:
        user_stream_ok = client.start_user_stream()
        print(f"[USER_STREAM] start_ok={user_stream_ok} status={client.get_user_stream_status()}")

    positions: Dict[str, PositionState] = {}
    trade_trackers: Dict[str, Dict[str, Any]] = {}
    pending_entries: Dict[str, Dict[str, Any]] = {}
    cooldown_until_ms: Dict[str, int] = {}
    last_force_exit_attempt_ms: Dict[str, int] = {}
    last_user_stream_warn_ms = 0
    daily_balance_day: Optional[str] = None
    daily_start_balance: Optional[float] = None
    daily_peak_balance: Optional[float] = None
    daily_last_balance: Optional[float] = None
    daily_drawdown_frac = 0.0
    daily_drawdown_blocked = False
    daily_drawdown_blocker_pct = args.daily_drawdown_blocker_pct
    daily_balance_missing_warned = False
    effective_order_notional = args.order_notional
    start = time.time()

    try:
        while (time.time() - start) < args.poll_time and not client._stop_event.is_set():
            ts_ms = _now_ms()
            utc_minute, utc_dt = _utc_minute_of_day(ts_ms)
            if order_placer is not None:
                client.poll_user_stream_maintenance(now_ms=ts_ms)
                user_status = client.get_user_stream_status(now_ms=ts_ms)
                if (not user_status.get("healthy")) and (ts_ms - last_user_stream_warn_ms >= 15_000):
                    last_user_stream_warn_ms = ts_ms
                    print(f"[USER_STREAM_WARN] unhealthy status={user_status}")

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

                balance_now = client.get_total_margin_balance(prefer_ws=True, fallback_rest=True)
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
                            and daily_drawdown_frac >= (daily_drawdown_blocker_pct / 100.0)
                        ):
                            daily_drawdown_blocked = True
                            print(
                                (
                                    f"[RISK_BLOCK] daily drawdown triggered "
                                    f"dd_pct={daily_drawdown_frac * 100.0:.3f} "
                                    f"threshold_pct={daily_drawdown_blocker_pct:.3f} "
                                    f"peak_balance={daily_peak_balance:.6f} "
                                    f"current_balance={balance_now:.6f} "
                                    f"utc_day={utc_day}"
                                )
                            )
                elif not daily_balance_missing_warned:
                    daily_balance_missing_warned = True
                    print("[RISK_WARN] could not read totalMarginBalance; daily drawdown blocker/default notional unavailable until balance is available.")

            symbol_rows = client.get_symbol_rows(
                lookback_seconds=1,
                include_user_state=(order_placer is not None),
                fallback_rest_for_user_state=(order_placer is not None),
            )

            if args.update_logs:
                client.logger.write_second(ts_ms, symbol_rows)

            for sym in symbols:
                snap = symbol_rows[sym]
                sym_cfg = symbol_params[sym]
                strat = strat_by_symbol[sym]
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

                account_state = snap.get("account") or {}
                pos = positions.get(sym)
                tracker = trade_trackers.get(sym)
                mark_px_now = _safe_float((snap.get("funding") or {}).get("mark_px"))
                exit_order_ids_by_reason: Dict[str, Optional[int]] = {}
                if pos is not None:
                    exit_order_ids_by_reason = {
                        "TP": pos.take_profit_order_id,
                        "SL": pos.stop_loss_order_id,
                        "TSL": pos.trailing_stop_order_id,
                    }
                if tracker is not None:
                    drain_trade_tracker_events(
                        client=client,
                        symbol=sym,
                        tracker=tracker,
                        exit_order_ids_by_reason=exit_order_ids_by_reason,
                        mark_px=mark_px_now,
                    )

                ws_pos_amt, ws_pos_qty, ws_has_position, ws_pos_side = resolve_tracker_position_state(
                    account_state=account_state,
                    tracker=tracker,
                )

                pending = pending_entries.get(sym)
                if pending is not None and pos is None:
                    pending_order_id = pending.get("entry_order_id")
                    pending_update = None
                    if tracker is not None and isinstance(tracker.get("entry_order_update"), dict):
                        pending_update = tracker.get("entry_order_update")
                    if pending_update is None:
                        pending_update = client.get_order_update(pending_order_id)

                    pending_status = str((pending_update or {}).get("status") or (tracker or {}).get("entry_order_status") or "").upper()
                    tracker_fill_qty = _safe_float((tracker or {}).get("fill_quantity"))
                    tracker_fill_px = _safe_float((tracker or {}).get("entry_fill_price"))
                    tracker_fill_time_ms = _safe_float((tracker or {}).get("entry_fill_time_ms"))
                    pending_cum_qty = tracker_fill_qty if tracker_fill_qty is not None and tracker_fill_qty > 0 else _safe_float((pending_update or {}).get("cum_filled_qty"))
                    pending_avg_px = tracker_fill_px or _safe_float((pending_update or {}).get("avg_price")) or _safe_float((pending_update or {}).get("last_price"))
                    if ws_has_position or (pending_cum_qty is not None and pending_cum_qty > 0):
                        recovered_qty = ws_pos_qty if ws_has_position and ws_pos_qty is not None else float(pending_cum_qty or 0.0)
                        recovered_side = ws_pos_side or str(pending.get("side") or "")
                        recovered_px = pending_avg_px or _safe_float(pending.get("entry_price_hint")) or 0.0
                        recovered_fill_time_ms = int(
                            tracker_fill_time_ms
                            or _safe_float((pending_update or {}).get("transaction_time_ms"))
                            or _safe_float((pending_update or {}).get("event_time_ms"))
                            or ts_ms
                        )
                        if recovered_qty > 0 and recovered_px > 0 and recovered_side in {"BUY", "SELL"}:
                            recovered_pos = PositionState(
                                symbol=sym,
                                side=recovered_side,
                                qty=float(recovered_qty),
                                entry_vwap_px=float(recovered_px),
                                opened_time_ms=recovered_fill_time_ms,
                                taker_order_id=pending_order_id,
                            )
                            try:
                                trigger_resp = order_placer.place_exit_triggers(
                                    pos=recovered_pos,
                                    take_profit_bps=float(pending.get("tp_bps") or 0.0),
                                    stop_loss_bps=float(pending.get("sl_bps") or 0.0),
                                    trailing_activation_bps=float(pending.get("activation_bps") or 0.0),
                                    trailing_callback_rate=float(pending.get("trailing_callback_rate") or 0.0),
                                )
                                recovered_pos.take_profit_order_id = trigger_resp.get("take_profit_order_id")
                                recovered_pos.stop_loss_order_id = trigger_resp.get("stop_loss_order_id")
                                recovered_pos.trailing_stop_order_id = trigger_resp.get("trailing_stop_order_id")
                            except Exception as e:
                                print(f"[ENTRY_RECOVERY_WARN] {sym} failed to arm exits on recovered position: {e}")

                            positions[sym] = recovered_pos
                            if tracker is None:
                                tracker = new_trade_tracker(
                                    symbol=sym,
                                    side=recovered_side,
                                    entry_send_time_ms=int(pending.get("entry_send_time_ms") or ts_ms),
                                    entry_mark_price=mark_px_now,
                                    client=client,
                                )
                                trade_trackers[sym] = tracker
                            tracker["entry_order_id"] = recovered_pos.taker_order_id
                            if tracker.get("entry_mark_price") is None:
                                tracker["entry_mark_price"] = mark_px_now
                            record_tracker_fill(
                                tracker,
                                fill_qty=recovered_pos.qty,
                                fill_price=recovered_pos.entry_vwap_px,
                                fill_time_ms=recovered_fill_time_ms,
                            )
                            pending_entries.pop(sym, None)
                            print(f"[ENTRY_RECOVERED_WS] {sym} position={recovered_pos}")
                            pos = recovered_pos
                            exit_order_ids_by_reason = {
                                "TP": recovered_pos.take_profit_order_id,
                                "SL": recovered_pos.stop_loss_order_id,
                                "TSL": recovered_pos.trailing_stop_order_id,
                            }
                            drain_trade_tracker_events(
                                client=client,
                                symbol=sym,
                                tracker=tracker,
                                exit_order_ids_by_reason=exit_order_ids_by_reason,
                                mark_px=mark_px_now,
                            )
                            ws_pos_amt, ws_pos_qty, ws_has_position, ws_pos_side = resolve_tracker_position_state(
                                account_state=account_state,
                                tracker=tracker,
                            )
                    elif pending_status in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED"} and not ws_has_position:
                        pending_entries.pop(sym, None)
                        trade_trackers.pop(sym, None)
                        print(f"[ENTRY_PENDING_CLOSED] {sym} order_id={pending_order_id} status={pending_status} no fills observed.")
                    elif ts_ms - int(pending.get("entry_send_time_ms") or ts_ms) >= 120_000 and not ws_has_position:
                        pending_entries.pop(sym, None)
                        trade_trackers.pop(sym, None)
                        print(f"[ENTRY_PENDING_EXPIRED] {sym} order_id={pending_order_id} no fill/position observed.")

                pos = positions.get(sym)
                tracker = trade_trackers.get(sym)
                if pos is not None:
                    if tracker is not None:
                        record_tracker_fill(
                            tracker,
                            fill_qty=pos.qty,
                            fill_price=pos.entry_vwap_px,
                            fill_time_ms=pos.opened_time_ms,
                        )
                        tracker.setdefault("seen_exit_order_ids", set())

                    exit_order_ids_by_reason = {
                        "TP": pos.take_profit_order_id,
                        "SL": pos.stop_loss_order_id,
                        "TSL": pos.trailing_stop_order_id,
                    }
                    detect = tracker.get("detected_exit") if isinstance(tracker, dict) else None
                    if not isinstance(detect, dict) or detect.get("order_id") is None:
                        detect = client.detect_filled_exit_from_ws(
                            symbol=sym,
                            order_ids_by_reason=exit_order_ids_by_reason,
                        )
                        if (
                            tracker is not None
                            and detect.get("order_id") is not None
                            and float(detect.get("filled_qty") or 0.0) > 0
                        ):
                            tracker["detected_exit"] = detect
                    detected_order_id = detect.get("order_id")
                    detected_filled_qty = float(detect.get("filled_qty") or 0.0)
                    if detected_order_id is not None and detected_filled_qty > 0:
                        already_seen = False
                        if tracker is not None:
                            seen_exit_ids = tracker.get("seen_exit_order_ids")
                            if isinstance(seen_exit_ids, set):
                                if detected_order_id in seen_exit_ids:
                                    already_seen = True
                                else:
                                    seen_exit_ids.add(detected_order_id)
                        if not already_seen:
                            print(
                                (
                                    f"[EXIT_TRIGGER_FILL_WS] {sym} detected_exit={detect} "
                                    f"ws_position_qty={ws_pos_qty}"
                                )
                            )
                            if not ws_has_position:
                                finalize_and_close_position(
                                    symbol=sym,
                                    pos=pos,
                                    tracker=tracker,
                                    exit_reason=detect.get("reason") or "UNKNOWN",
                                    exit_order_id=detected_order_id,
                                    exit_send_time_ms_hint=None,
                                    exit_fill_price_hint=_safe_float(detect.get("avg_price")),
                                    exit_fill_time_ms_hint=(int(detect.get("update_time_ms")) if detect.get("update_time_ms") is not None else None),
                                    order_placer=order_placer,
                                    log_dir=args.log_dir,
                                    send_trade_alert_email=args.trade_alert_email,
                                    positions=positions,
                                    trade_trackers=trade_trackers,
                                    cooldown_until_ms=cooldown_until_ms,
                                    reentry_cooldown_min=args.reentry_cooldown_min,
                                    now_ms=ts_ms,
                                )
                                continue
                            if ws_pos_qty is not None and ws_pos_qty > 0:
                                pos.qty = float(ws_pos_qty)

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
                                    f"{daily_drawdown_blocker_pct:.4f}"
                                ),
                            )
                            print(f"[EXIT] {sym} {exit_res}")
                            if exit_res.ok:
                                finalize_and_close_position(
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
                                    positions=positions,
                                    trade_trackers=trade_trackers,
                                    cooldown_until_ms=cooldown_until_ms,
                                    reentry_cooldown_min=args.reentry_cooldown_min,
                                    now_ms=ts_ms,
                                )
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
                                finalize_and_close_position(
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
                                    positions=positions,
                                    trade_trackers=trade_trackers,
                                    cooldown_until_ms=cooldown_until_ms,
                                    reentry_cooldown_min=args.reentry_cooldown_min,
                                    now_ms=ts_ms,
                                )
                        continue

                    if not ws_has_position:
                        print(f"[POSITION] {sym} appears closed on exchange via WS. detected_exit={detect}")
                        finalize_and_close_position(
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
                            positions=positions,
                            trade_trackers=trade_trackers,
                            cooldown_until_ms=cooldown_until_ms,
                            reentry_cooldown_min=args.reentry_cooldown_min,
                            now_ms=ts_ms,
                        )
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
                            finalize_and_close_position(
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
                                positions=positions,
                                trade_trackers=trade_trackers,
                                cooldown_until_ms=cooldown_until_ms,
                                reentry_cooldown_min=args.reentry_cooldown_min,
                                now_ms=ts_ms,
                            )
                    continue

                if daily_drawdown_blocked:
                    if ws_has_position and ws_pos_side is not None:
                        last_attempt = last_force_exit_attempt_ms.get(sym, 0)
                        if ts_ms - last_attempt >= 10_000:
                            last_force_exit_attempt_ms[sym] = ts_ms
                            temp_pos = PositionState(
                                symbol=sym,
                                side=ws_pos_side,
                                qty=float(ws_pos_qty or 0.0),
                                entry_vwap_px=0.0,
                                opened_time_ms=ts_ms,
                            )
                            exit_res = order_placer.close_position(
                                pos=temp_pos,
                                price_source=client,
                                reason="DAILY_DRAWDOWN_BLOCK",
                                notes=(
                                    f"dd_pct={daily_drawdown_frac * 100.0:.4f} >= "
                                    f"{daily_drawdown_blocker_pct:.4f}"
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
                            f"threshold_pct={daily_drawdown_blocker_pct:.4f}"
                        )
                    )
                    continue

                if utc_minute >= force_exit_min:
                    if ws_has_position and ws_pos_side is not None:
                        last_attempt = last_force_exit_attempt_ms.get(sym, 0)
                        if ts_ms - last_attempt >= 10_000:
                            last_force_exit_attempt_ms[sym] = ts_ms
                            temp_pos = PositionState(
                                symbol=sym,
                                side=ws_pos_side,
                                qty=float(ws_pos_qty or 0.0),
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

                if not bool(sym_cfg.get("enable_trading", True)):
                    print(f"[ENTRY_BLOCKED] {sym} trading disabled in config_current")
                    continue

                if utc_minute >= entry_halt_min:
                    print(f"[ENTRY_BLOCKED] {sym} entry_halt_utc={args.entry_halt_utc} now_utc={utc_dt.isoformat()}")
                    continue

                if ts_ms < cooldown_until_ms.get(sym, 0):
                    remaining_s = (cooldown_until_ms[sym] - ts_ms) // 1000
                    print(f"[ENTRY_BLOCKED] {sym} cooldown active, remaining={remaining_s}s")
                    continue

                if ws_has_position:
                    print(f"[ENTRY_BLOCKED] {sym} exchange position still open qty={ws_pos_qty} source={account_state.get('source')}")
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
                activation_auto_bps = be_floor_bps + max(0.0, sym_cfg["activation_buffer_bps"])
                activation_bps = max(sym_cfg["activation_bps"], activation_auto_bps)
                tp_bps = max(sym_cfg["tp_bps"], activation_bps + max(0.0, sym_cfg["min_tp_gap_bps"]))
                sl_bps = sym_cfg["sl_bps"]
                trailing_callback_rate = sym_cfg["callback_bps"] / 1e4

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
                draft_tracker = new_trade_tracker(
                    symbol=sym,
                    side=side,
                    entry_send_time_ms=entry_send_ms,
                    entry_mark_price=mark_px_now,
                    client=client,
                )
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
                entry_order_id = entry_res.taker_order_id
                if pos is not None or entry_order_id is not None:
                    draft_tracker["entry_order_id"] = entry_order_id
                    trade_trackers[sym] = draft_tracker
                    initial_fill_time_ms = _extract_update_time_ms(entry_res.raw.get("taker_query") if isinstance(entry_res.raw, dict) else None)
                    if pos is not None:
                        record_tracker_fill(
                            draft_tracker,
                            fill_qty=pos.qty,
                            fill_price=pos.entry_vwap_px,
                            fill_time_ms=(initial_fill_time_ms or _now_ms()),
                        )
                    entry_exit_order_ids_by_reason = {}
                    if pos is not None:
                        entry_exit_order_ids_by_reason = {
                            "TP": pos.take_profit_order_id,
                            "SL": pos.stop_loss_order_id,
                            "TSL": pos.trailing_stop_order_id,
                        }
                    drain_trade_tracker_events(
                        client=client,
                        symbol=sym,
                        tracker=draft_tracker,
                        exit_order_ids_by_reason=entry_exit_order_ids_by_reason,
                        mark_px=mark_px_now,
                    )
                if pos is not None:
                    positions[sym] = pos
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
                else:
                    pending_order_id = entry_order_id
                    if pending_order_id is not None:
                        pending_entries[sym] = {
                            "entry_order_id": pending_order_id,
                            "entry_send_time_ms": entry_send_ms,
                            "entry_price_hint": entry_limit_price,
                            "side": side,
                            "tp_bps": tp_bps,
                            "sl_bps": sl_bps,
                            "activation_bps": activation_bps,
                            "trailing_callback_rate": trailing_callback_rate,
                        }
                        print(f"[ENTRY_PENDING] {sym} order_id={pending_order_id} awaiting WS fill/position update.")
                    else:
                        trade_trackers.pop(sym, None)

            client.n_poll_snapshots += 1
            time.sleep(client.poll_seconds)
    finally:
        client._stop_event.set()
        client.logger.close()
        client.graceful_shutdown(handshake_wait_seconds=0.8)

    print("done:", {"n_poll_snapshots": client.n_poll_snapshots})
    print(f"logs written to {args.log_dir}")
