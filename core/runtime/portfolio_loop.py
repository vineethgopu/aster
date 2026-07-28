from __future__ import annotations

import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from exchange.client import AsterClient
from execution.order import OrderPlacer, PositionState
from signals.cross_sectional_momentum import CrossSectionalMomentumStrategy
from .utils import _now_ms, _safe_float


PORTFOLIO_REBALANCE_FIELDS = [
    "ts_unix_ms",
    "ts_utc",
    "strategy",
    "symbol",
    "equity_usd",
    "target_weight",
    "target_notional_usd",
    "price",
    "current_signed_qty",
    "target_signed_qty",
    "action",
    "order_side",
    "requested_delta_qty",
    "filled_delta_qty",
    "filled_qty",
    "fill_price",
    "order_id",
    "reduce_only",
    "ok",
    "notes",
]


def _fmt_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def _append_rebalance_rows(log_dir: str, rows: list[Dict[str, Any]]) -> None:
    if not rows:
        return
    ts_ms = int(rows[0].get("ts_unix_ms") or _now_ms())
    day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")
    path = Path(log_dir) / f"portfolio_rebalance_{day}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = (not path.exists()) or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PORTFOLIO_REBALANCE_FIELDS)
        if needs_header:
            w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in PORTFOLIO_REBALANCE_FIELDS})


def _mark_or_mid(snap: Dict[str, Any]) -> Optional[float]:
    funding = snap.get("funding") or {}
    mark = _safe_float(funding.get("mark_px"))
    if mark is not None and mark > 0:
        return mark
    bbo = snap.get("bbo") or {}
    mid = _safe_float(bbo.get("mid"))
    if mid is not None and mid > 0:
        return mid
    bid = _safe_float(bbo.get("bid_px"))
    ask = _safe_float(bbo.get("ask_px"))
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return 0.5 * (bid + ask)
    return None


def _position_side_from_signed_qty(qty: float) -> str:
    return "BUY" if qty > 0 else "SELL"


def _opposite_sign(a: float, b: float) -> bool:
    return (a > 0 and b < 0) or (a < 0 and b > 0)


def _target_signed_qty(
    *,
    order_placer: OrderPlacer,
    symbol: str,
    target_notional_usd: float,
    price: float,
) -> float:
    qty = order_placer.compute_qty_for_target_notional(
        symbol=symbol,
        price=price,
        target_notional_usd=target_notional_usd,
    )
    if qty <= 0:
        return 0.0
    return qty if target_notional_usd > 0 else -qty


def _build_rebalance_row(
    *,
    ts_ms: int,
    symbol: str,
    equity_usd: float,
    target_weight: float,
    target_notional_usd: float,
    price: Optional[float],
    current_signed_qty: Optional[float],
    target_signed_qty: Optional[float],
    action: str,
    result: Any = None,
    notes: str = "",
) -> Dict[str, Any]:
    return {
        "ts_unix_ms": ts_ms,
        "ts_utc": _fmt_utc(ts_ms),
        "strategy": "cross_sectional_momentum",
        "symbol": symbol,
        "equity_usd": equity_usd,
        "target_weight": target_weight,
        "target_notional_usd": target_notional_usd,
        "price": price,
        "current_signed_qty": current_signed_qty,
        "target_signed_qty": target_signed_qty,
        "action": action,
        "order_side": getattr(result, "side", None),
        "requested_delta_qty": (None if target_signed_qty is None or current_signed_qty is None else target_signed_qty - current_signed_qty),
        "filled_delta_qty": getattr(result, "delta_qty", None),
        "filled_qty": getattr(result, "filled_qty", None),
        "fill_price": getattr(result, "vwap_fill_px", None),
        "order_id": getattr(result, "order_id", None),
        "reduce_only": getattr(result, "reduce_only", None),
        "ok": getattr(result, "ok", None),
        "notes": notes or getattr(result, "notes", ""),
    }


def execute_portfolio_rebalance(
    *,
    client: AsterClient,
    order_placer: Optional[OrderPlacer],
    strategy: CrossSectionalMomentumStrategy,
    symbol_rows: Dict[str, Dict[str, Any]],
    target_weights: Dict[str, float],
    equity_usd: float,
    enable_trading: bool,
    log_dir: str,
    ts_ms: int,
) -> None:
    rows: list[Dict[str, Any]] = []
    for symbol in strategy.cfg.symbols:
        snap = symbol_rows.get(symbol) or {}
        price = _mark_or_mid(snap)
        target_weight = float(target_weights.get(symbol, 0.0))
        target_notional = equity_usd * target_weight
        account = snap.get("account") or {}
        current_signed_qty = _safe_float(account.get("position_amt"))
        if current_signed_qty is None and hasattr(client, "get_position_amt"):
            current_signed_qty = client.get_position_amt(symbol, prefer_ws=True, fallback_rest=True)
        if current_signed_qty is None and not enable_trading:
            current_signed_qty = 0.0

        if price is None or price <= 0:
            rows.append(
                _build_rebalance_row(
                    ts_ms=ts_ms,
                    symbol=symbol,
                    equity_usd=equity_usd,
                    target_weight=target_weight,
                    target_notional_usd=target_notional,
                    price=price,
                    current_signed_qty=current_signed_qty,
                    target_signed_qty=None,
                    action="SKIP",
                    notes="missing mark/mid price",
                )
            )
            continue
        if current_signed_qty is None:
            rows.append(
                _build_rebalance_row(
                    ts_ms=ts_ms,
                    symbol=symbol,
                    equity_usd=equity_usd,
                    target_weight=target_weight,
                    target_notional_usd=target_notional,
                    price=price,
                    current_signed_qty=None,
                    target_signed_qty=None,
                    action="SKIP",
                    notes="missing current signed position",
                )
            )
            continue

        target_signed_qty = _target_signed_qty(
            order_placer=order_placer,
            symbol=symbol,
            target_notional_usd=target_notional,
            price=price,
        ) if order_placer is not None else target_notional / price

        if not enable_trading or order_placer is None:
            rows.append(
                _build_rebalance_row(
                    ts_ms=ts_ms,
                    symbol=symbol,
                    equity_usd=equity_usd,
                    target_weight=target_weight,
                    target_notional_usd=target_notional,
                    price=price,
                    current_signed_qty=current_signed_qty,
                    target_signed_qty=target_signed_qty,
                    action="DRY_RUN",
                    notes="trading disabled",
                )
            )
            continue

        current_for_delta = float(current_signed_qty)
        if abs(current_for_delta) > 0 and _opposite_sign(current_for_delta, target_signed_qty):
            close_pos = PositionState(
                symbol=symbol,
                side=_position_side_from_signed_qty(current_for_delta),
                qty=abs(current_for_delta),
                entry_vwap_px=0.0,
                opened_time_ms=ts_ms,
            )
            close_result = order_placer.close_position(
                pos=close_pos,
                price_source=client,
                reason="PORTFOLIO_REBALANCE_FLIP",
                notes=f"target_signed_qty={target_signed_qty}",
                order_type="LIMIT",
                refresh_from_exchange=True,
            )
            rows.append(
                _build_rebalance_row(
                    ts_ms=ts_ms,
                    symbol=symbol,
                    equity_usd=equity_usd,
                    target_weight=target_weight,
                    target_notional_usd=target_notional,
                    price=price,
                    current_signed_qty=current_for_delta,
                    target_signed_qty=0.0,
                    action="FLIP_CLOSE",
                    result=close_result,
                )
            )
            if not close_result.ok:
                continue
            refreshed = client.get_position_amt(symbol, prefer_ws=True, fallback_rest=True)
            current_for_delta = float(refreshed or 0.0)

        result = order_placer.rebalance_delta(
            symbol=symbol,
            current_signed_qty=current_for_delta,
            target_signed_qty=target_signed_qty,
            price_source=client,
            min_delta_notional_usd=strategy.cfg.min_delta_notional_usd,
        )
        rows.append(
            _build_rebalance_row(
                ts_ms=ts_ms,
                symbol=symbol,
                equity_usd=equity_usd,
                target_weight=target_weight,
                target_notional_usd=target_notional,
                price=price,
                current_signed_qty=current_for_delta,
                target_signed_qty=target_signed_qty,
                action="REBALANCE_DELTA",
                result=result,
            )
        )

    _append_rebalance_rows(log_dir=log_dir, rows=rows)
    for row in rows:
        print(f"[PORTFOLIO_REBALANCE_ROW] {row}")


def run_portfolio_runtime(
    *,
    client: AsterClient,
    order_placer: Optional[OrderPlacer],
    strategy: CrossSectionalMomentumStrategy,
    log_dir: str,
    poll_time: int,
    update_logs: bool,
    enable_trading: bool,
    daily_drawdown_blocker_pct: float,
    margin_safety_multiple: float,
    dry_run_equity_usd: Optional[float] = None,
) -> None:
    startup = client.rest_snapshot()
    client._seed_from_rest_snapshot(startup)
    client.ws.start()
    streams = client._build_combined_streams()
    client.ws.live_subscribe(streams, id=1, callback=client._on_ws_message)
    if order_placer is not None:
        user_stream_ok = client.start_user_stream()
        print(f"[USER_STREAM] start_ok={user_stream_ok} status={client.get_user_stream_status()}")

    daily_balance_day: Optional[str] = None
    daily_peak_balance: Optional[float] = None
    daily_drawdown_blocked = False
    start = time.time()

    try:
        while (time.time() - start) < poll_time and not client._stop_event.is_set():
            ts_ms = _now_ms()
            equity = client.get_total_margin_balance(prefer_ws=True, fallback_rest=True) if order_placer is not None else None
            if (equity is None or equity <= 0) and not enable_trading and dry_run_equity_usd is not None:
                equity = float(dry_run_equity_usd)
            if equity is not None and equity > 0:
                utc_day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date().isoformat()
                if utc_day != daily_balance_day:
                    daily_balance_day = utc_day
                    daily_peak_balance = equity
                    daily_drawdown_blocked = False
                    print(f"[PORTFOLIO_RISK_DAY_RESET] utc_day={utc_day} equity={equity:.6f}")
                daily_peak_balance = max(float(daily_peak_balance or equity), float(equity))
                dd = max(0.0, (daily_peak_balance - equity) / daily_peak_balance) if daily_peak_balance > 0 else 0.0
                if dd >= daily_drawdown_blocker_pct / 100.0:
                    daily_drawdown_blocked = True
                    print(
                        f"[PORTFOLIO_RISK_BLOCK] daily_drawdown_pct={dd * 100.0:.4f} "
                        f"threshold_pct={daily_drawdown_blocker_pct:.4f}"
                    )

            symbol_rows = client.get_symbol_rows(
                lookback_seconds=1,
                include_user_state=(order_placer is not None),
                fallback_rest_for_user_state=(order_placer is not None),
            )
            if update_logs:
                client.logger.write_second(ts_ms, symbol_rows)

            decision = strategy.on_second(symbol_rows=symbol_rows, now_ms=ts_ms)
            if decision:
                print(f"[PORTFOLIO_SIGNAL] {decision}")

            if decision and decision.get("enter"):
                if daily_drawdown_blocked:
                    print("[PORTFOLIO_REBALANCE_SKIP] daily drawdown blocker active")
                elif equity is None or equity <= 0:
                    print("[PORTFOLIO_REBALANCE_SKIP] equity unavailable")
                else:
                    execute_portfolio_rebalance(
                        client=client,
                        order_placer=order_placer,
                        strategy=strategy,
                        symbol_rows=symbol_rows,
                        target_weights=decision.get("target_weights") or {},
                        equity_usd=float(equity),
                        enable_trading=bool(enable_trading and strategy.cfg.enable_trading),
                        log_dir=log_dir,
                        ts_ms=ts_ms,
                    )

            if order_placer is not None:
                # Preserve the old margin kill concept as a monitor. Full forced
                # close-all can be added here once account safety semantics are verified.
                safety = order_placer._get_margin_safety_multiple_total_usdt()
                if safety is not None and safety > 0 and safety <= margin_safety_multiple:
                    print(
                        f"[PORTFOLIO_MARGIN_WARN] safety_multiple={safety:.4f} "
                        f"threshold={margin_safety_multiple:.4f}"
                    )

            time.sleep(1.0)
    finally:
        client.close()
