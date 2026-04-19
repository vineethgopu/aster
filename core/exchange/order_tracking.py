from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from .client_types import (
    AGG_TRADE_EVENT_BUFFER_SIZE,
    AGG_TRADE_EVENT_BUFFER_TRIM,
    PRIVATE_EVENT_BUFFER_SIZE,
    PRIVATE_EVENT_BUFFER_TRIM,
    WsOrderUpdate,
    WsPosition,
    _now_ms,
    _to_bool_or_none,
    _to_float,
    _to_int,
)


class OrderTrackingMixin:
    def _extract_listen_key(self, payload: Any) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        if payload.get("listenKey"):
            return str(payload.get("listenKey"))
        data = payload.get("data")
        if isinstance(data, dict) and data.get("listenKey"):
            return str(data.get("listenKey"))
        return None

    def _is_user_stream_healthy_unlocked(self, now_ms: Optional[int] = None) -> bool:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        if not self.user_stream_enabled:
            return False
        if not self.user_stream_subscribed:
            return False
        if not self.user_stream_listen_key:
            return False
        if self.user_stream_expired:
            return False
        if self.user_stream_last_error:
            return False
        if self.user_stream_last_keepalive_ms > 0:
            max_age_ms = 2 * self.user_stream_keepalive_seconds * 1000
            if now_ms - self.user_stream_last_keepalive_ms > max_age_ms:
                return False
        return True

    def get_user_stream_status(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        with self._lock:
            return {
                "enabled": bool(self.user_stream_enabled),
                "subscribed": bool(self.user_stream_subscribed),
                "healthy": bool(self._is_user_stream_healthy_unlocked(now_ms=now_ms)),
                "listen_key_present": bool(self.user_stream_listen_key),
                "expired": bool(self.user_stream_expired),
                "last_error": self.user_stream_last_error,
                "last_event_ms": self.user_stream_last_event_ms or None,
                "last_account_event_ms": self.user_stream_last_account_event_ms or None,
                "last_order_event_ms": self.user_stream_last_order_event_ms or None,
                "last_keepalive_ms": self.user_stream_last_keepalive_ms or None,
                "age_last_event_ms": (now_ms - self.user_stream_last_event_ms) if self.user_stream_last_event_ms > 0 else None,
                "age_last_keepalive_ms": (now_ms - self.user_stream_last_keepalive_ms) if self.user_stream_last_keepalive_ms > 0 else None,
            }

    def reconcile_account_state_from_rest(self, force: bool = False, merge_into_ws: bool = False) -> bool:
        if not self._has_auth:
            return False
        now_ms = _now_ms()
        interval_ms = self.rest_reconcile_seconds * 1000
        if (not force) and (now_ms - self._rest_last_reconcile_ms < interval_ms):
            return True

        positions_out: Dict[str, tuple[float, Optional[float], Optional[float], Optional[str], Optional[float], int]] = {}
        total_margin_balance: Optional[float] = None

        try:
            resp = self.rest.get_position_risk(recvWindow=6000)
            data = resp.get("data") if isinstance(resp, dict) and isinstance(resp.get("data"), (list, dict)) else resp
            rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            for row in rows:
                if not isinstance(row, dict):
                    continue
                sym = str(row.get("symbol", "")).upper()
                if not sym:
                    continue
                amt = _to_float(row.get("positionAmt"))
                if amt is None:
                    continue
                entry = _to_float(row.get("entryPrice"))
                upnl = _to_float(row.get("unRealizedProfit"))
                mt = row.get("marginType")
                iw = _to_float(row.get("isolatedWallet"))
                upd = _to_int(row.get("updateTime")) or now_ms
                positions_out[sym] = (float(amt), entry, upnl, str(mt).upper() if mt is not None else None, iw, int(upd))
        except Exception as e:
            logging.warning(f"[CLIENT_RECONCILE] get_position_risk failed: {e}")

        try:
            acct = self.rest.account(recvWindow=6000)
            d = acct.get("data") if isinstance(acct, dict) and isinstance(acct.get("data"), dict) else acct
            mb = _to_float((d or {}).get("totalMarginBalance")) if isinstance(d, dict) else None
            if mb is not None and mb > 0:
                total_margin_balance = float(mb)
        except Exception as e:
            logging.warning(f"[CLIENT_RECONCILE] account failed: {e}")

        with self._lock:
            for sym in self.symbols:
                self._rest_positions_by_symbol[sym] = 0.0
            for sym, packed in positions_out.items():
                self._rest_positions_by_symbol[sym] = float(packed[0])
            self._rest_last_reconcile_ms = now_ms
            self._rest_total_margin_balance = total_margin_balance

            if merge_into_ws:
                self._ws_account_seeded_from_rest = True
                self.user_positions = {}
                for sym, packed in positions_out.items():
                    amt, entry, upnl, mt, iw, upd = packed
                    if abs(amt) <= 0:
                        continue
                    self.user_positions[sym] = WsPosition(
                        symbol=sym,
                        event_time_ms=int(upd),
                        position_amt=float(amt),
                        entry_price=entry,
                        unrealized_pnl=upnl,
                        margin_type=mt,
                        isolated_wallet=iw,
                    )
                if total_margin_balance is not None:
                    self.user_balances["USDT"] = {
                        "asset": "USDT",
                        "wallet_balance": total_margin_balance,
                        "cross_wallet_balance": total_margin_balance,
                        "balance_change": None,
                        "event_time_ms": now_ms,
                    }

        return True

    def start_user_stream(self) -> bool:
        if not self._has_auth:
            with self._lock:
                self.user_stream_enabled = False
            return False

        self.reconcile_account_state_from_rest(force=True, merge_into_ws=True)

        try:
            key_resp = self.rest.new_listen_key()
            listen_key = self._extract_listen_key(key_resp)
            if not listen_key:
                raise RuntimeError(f"listenKey missing in response: {key_resp}")
        except Exception as e:
            with self._lock:
                self.user_stream_last_error = f"new_listen_key failed: {e}"
                self.user_stream_enabled = False
                self.user_stream_subscribed = False
                self.user_stream_expired = False
            logging.warning(f"[USER_STREAM] failed to create listen key: {e}")
            return False

        now_ms = _now_ms()
        with self._lock:
            self.user_stream_enabled = True
            self.user_stream_subscribed = False
            self.user_stream_expired = False
            self.user_stream_last_error = None
            self.user_stream_listen_key = listen_key
            self.user_stream_started_ms = now_ms
            self.user_stream_last_keepalive_ms = now_ms
            self.user_stream_next_keepalive_ms = now_ms + self.user_stream_keepalive_seconds * 1000
            self.user_stream_last_restart_attempt_ms = now_ms

        try:
            self.ws.user_data(
                listen_key=listen_key,
                id=9001,
                callback=self._on_user_ws_message,
            )
            with self._lock:
                self.user_stream_subscribed = True
        except Exception as e:
            with self._lock:
                self.user_stream_last_error = f"user_data subscribe failed: {e}"
                self.user_stream_subscribed = False
            logging.warning(f"[USER_STREAM] failed to subscribe listen key stream: {e}")
            return False

        logging.info("[USER_STREAM] subscribed successfully.")
        return True

    def close_user_stream(self) -> None:
        with self._lock:
            lk = self.user_stream_listen_key
            self.user_stream_enabled = False
            self.user_stream_subscribed = False
            self.user_stream_expired = False
            self.user_stream_last_error = None
            self.user_stream_listen_key = None
        if lk and self._has_auth:
            try:
                self.rest.close_listen_key(lk)
            except Exception:
                pass

    def poll_user_stream_maintenance(self, now_ms: Optional[int] = None) -> None:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        if not self._has_auth:
            return

        with self._lock:
            enabled = self.user_stream_enabled
            subscribed = self.user_stream_subscribed
            lk = self.user_stream_listen_key
            next_keepalive = self.user_stream_next_keepalive_ms
            expired = self.user_stream_expired
            last_error = self.user_stream_last_error
            last_event_ms = self.user_stream_last_event_ms
            last_restart_ms = self.user_stream_last_restart_attempt_ms

        if enabled and subscribed and lk and now_ms >= next_keepalive:
            try:
                self.rest.renew_listen_key(lk)
                with self._lock:
                    self.user_stream_last_keepalive_ms = now_ms
                    self.user_stream_next_keepalive_ms = now_ms + self.user_stream_keepalive_seconds * 1000
                    self.user_stream_last_error = None
            except Exception as e:
                with self._lock:
                    self.user_stream_last_error = f"renew_listen_key failed: {e}"
                logging.warning(f"[USER_STREAM] keepalive failed: {e}")

        unhealthy = bool(expired or last_error or (enabled and not subscribed))
        if unhealthy:
            self.reconcile_account_state_from_rest(force=True, merge_into_ws=True)
            cooldown_ms = self.user_stream_restart_cooldown_seconds * 1000
            if now_ms - last_restart_ms >= cooldown_ms:
                logging.warning("[USER_STREAM] unhealthy, attempting restart.")
                self.start_user_stream()

        if enabled and subscribed and last_event_ms > 0:
            stale_age_ms = now_ms - last_event_ms
            stale_warn_ms = self.user_stream_stale_warn_seconds * 1000
            if stale_age_ms >= stale_warn_ms:
                with self._lock:
                    if now_ms - self._last_user_stale_warn_ms >= stale_warn_ms:
                        self._last_user_stale_warn_ms = now_ms
                        logging.warning(
                            f"[USER_STREAM] no user events for {stale_age_ms}ms; refreshing REST account snapshot."
                        )
                self.reconcile_account_state_from_rest(force=True, merge_into_ws=True)

    def _append_user_event_unlocked(self, event: Dict[str, Any]) -> None:
        self.recent_user_events.append(event)
        if len(self.recent_user_events) > 2000:
            del self.recent_user_events[:1000]

    def _next_private_event_seq_unlocked(self) -> int:
        self._private_event_seq += 1
        return int(self._private_event_seq)

    def _next_agg_trade_event_seq_unlocked(self) -> int:
        self._agg_trade_event_seq += 1
        return int(self._agg_trade_event_seq)

    def _append_private_event_unlocked(self, symbol: str, event: Dict[str, Any]) -> None:
        sym = str(symbol).upper()
        if not sym:
            return
        row = dict(event)
        row["symbol"] = sym
        row["seq"] = self._next_private_event_seq_unlocked()
        buf = self.private_events_by_symbol.setdefault(sym, [])
        buf.append(row)
        if len(buf) > PRIVATE_EVENT_BUFFER_SIZE:
            del buf[:PRIVATE_EVENT_BUFFER_TRIM]

    def _append_agg_trade_event_unlocked(self, symbol: str, trade: Any) -> None:
        sym = str(symbol).upper()
        if not sym:
            return
        row = asdict(trade)
        row["seq"] = self._next_agg_trade_event_seq_unlocked()
        buf = self.agg_trade_events.setdefault(sym, [])
        buf.append(row)
        if len(buf) > AGG_TRADE_EVENT_BUFFER_SIZE:
            del buf[:AGG_TRADE_EVENT_BUFFER_TRIM]

    def _on_user_ws_message(self, msg: Dict[str, Any]) -> None:
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return
        etype = str(data.get("e") or "").strip()
        now_ms = _now_ms()
        ev_ms = _to_int(data.get("E")) or now_ms

        with self._lock:
            self.user_stream_last_event_ms = max(self.user_stream_last_event_ms, int(ev_ms))
            self.user_stream_enabled = True
            self.user_stream_subscribed = True
            self._append_user_event_unlocked(
                {
                    "event_type": etype or "UNKNOWN",
                    "event_time_ms": int(ev_ms),
                    "recv_time_ms": now_ms,
                }
            )

            if etype == "ACCOUNT_UPDATE":
                self.user_stream_last_account_event_ms = max(self.user_stream_last_account_event_ms, int(ev_ms))
                self._apply_account_update_unlocked(data=data, ev_ms=int(ev_ms))
                self.user_stream_last_error = None
            elif etype == "ORDER_TRADE_UPDATE":
                self.user_stream_last_order_event_ms = max(self.user_stream_last_order_event_ms, int(ev_ms))
                self._apply_order_update_unlocked(data=data, ev_ms=int(ev_ms))
                self.user_stream_last_error = None
            elif etype == "listenKeyExpired":
                self.user_stream_expired = True
                self.user_stream_last_error = "listenKeyExpired"
            elif etype == "error":
                self.user_stream_last_error = str(data.get("m") or "user stream error")

    def _apply_account_update_unlocked(self, data: Dict[str, Any], ev_ms: int) -> None:
        acct = data.get("a") if isinstance(data.get("a"), dict) else data
        balances = acct.get("B") if isinstance(acct, dict) and isinstance(acct.get("B"), list) else []
        positions = acct.get("P") if isinstance(acct, dict) and isinstance(acct.get("P"), list) else []

        for b in balances:
            if not isinstance(b, dict):
                continue
            asset = str(b.get("a", b.get("asset", ""))).upper()
            if not asset:
                continue
            wallet_balance = _to_float(b.get("wb", b.get("walletBalance")))
            cross_wallet = _to_float(b.get("cw", b.get("crossWalletBalance")))
            balance_change = _to_float(b.get("bc", b.get("balanceChange")))
            self.user_balances[asset] = {
                "asset": asset,
                "wallet_balance": wallet_balance,
                "cross_wallet_balance": cross_wallet,
                "balance_change": balance_change,
                "event_time_ms": int(ev_ms),
            }

        for p in positions:
            if not isinstance(p, dict):
                continue
            sym = str(p.get("s", p.get("symbol", ""))).upper()
            if not sym:
                continue
            amt = _to_float(p.get("pa", p.get("positionAmt")))
            if amt is None:
                continue
            entry = _to_float(p.get("ep", p.get("entryPrice")))
            upnl = _to_float(p.get("up", p.get("unRealizedProfit")))
            mt = p.get("mt", p.get("marginType"))
            iw = _to_float(p.get("iw", p.get("isolatedWallet")))

            if abs(float(amt)) <= 0:
                self.user_positions.pop(sym, None)
            else:
                self.user_positions[sym] = WsPosition(
                    symbol=sym,
                    event_time_ms=int(ev_ms),
                    position_amt=float(amt),
                    entry_price=entry,
                    unrealized_pnl=upnl,
                    margin_type=(str(mt).upper() if mt is not None else None),
                    isolated_wallet=iw,
                )
            self._append_private_event_unlocked(
                sym,
                {
                    "event_type": "ACCOUNT_POSITION",
                    "event_time_ms": int(ev_ms),
                    "position_amt": float(amt),
                    "entry_price": entry,
                    "unrealized_pnl": upnl,
                    "margin_type": (str(mt).upper() if mt is not None else None),
                    "isolated_wallet": iw,
                },
            )

        self._ws_account_seeded_from_rest = True

    def _apply_order_update_unlocked(self, data: Dict[str, Any], ev_ms: int) -> None:
        o = data.get("o") if isinstance(data.get("o"), dict) else data
        if not isinstance(o, dict):
            return

        sym = str(o.get("s", o.get("symbol", ""))).upper()
        oid = _to_int(o.get("i", o.get("orderId")))
        if not sym or oid is None:
            return

        tx_ms = _to_int(o.get("T", o.get("transactionTime")))
        existing = self.user_orders_by_id.get(int(oid))
        if existing is not None and existing.event_time_ms > int(ev_ms):
            return

        upd = WsOrderUpdate(
            symbol=sym,
            order_id=int(oid),
            client_order_id=(str(o.get("c")) if o.get("c") is not None else str(o.get("clientOrderId")) if o.get("clientOrderId") is not None else None),
            event_time_ms=int(ev_ms),
            transaction_time_ms=int(tx_ms) if tx_ms is not None else None,
            side=(str(o.get("S")) if o.get("S") is not None else str(o.get("side")) if o.get("side") is not None else None),
            order_type=(str(o.get("o")) if o.get("o") is not None else str(o.get("type")) if o.get("type") is not None else None),
            time_in_force=(str(o.get("f")) if o.get("f") is not None else str(o.get("timeInForce")) if o.get("timeInForce") is not None else None),
            status=(str(o.get("X")) if o.get("X") is not None else str(o.get("status")) if o.get("status") is not None else None),
            execution_type=(str(o.get("x")) if o.get("x") is not None else str(o.get("executionType")) if o.get("executionType") is not None else None),
            orig_qty=_to_float(o.get("q", o.get("origQty"))),
            cum_filled_qty=_to_float(o.get("z", o.get("executedQty"))),
            last_filled_qty=_to_float(o.get("l", o.get("lastFilledQty"))),
            avg_price=_to_float(o.get("ap", o.get("avgPrice"))),
            last_price=_to_float(o.get("L", o.get("lastPrice"))),
            reduce_only=(_to_bool_or_none(o.get("R")) if o.get("R") is not None else _to_bool_or_none(o.get("reduceOnly"))),
            close_position=(_to_bool_or_none(o.get("cp")) if o.get("cp") is not None else _to_bool_or_none(o.get("closePosition"))),
            raw=dict(o),
        )
        self.user_orders_by_id[int(oid)] = upd
        by_symbol = self.user_orders_by_symbol.setdefault(sym, {})
        by_symbol[int(oid)] = upd
        recent = self.recent_order_updates.setdefault(sym, [])
        recent.append(upd)
        if len(recent) > 3000:
            del recent[:1500]
        self._append_private_event_unlocked(
            sym,
            {
                "event_type": "ORDER_TRADE_UPDATE",
                "event_time_ms": int(ev_ms),
                "order_update": asdict(upd),
            },
        )

    def get_order_update(self, order_id: Optional[int]) -> Optional[Dict[str, Any]]:
        if order_id is None:
            return None
        with self._lock:
            upd = self.user_orders_by_id.get(int(order_id))
            return asdict(upd) if upd is not None else None

    def get_latest_private_event_seq(self, symbol: str) -> int:
        sym = str(symbol).upper()
        with self._lock:
            buf = self.private_events_by_symbol.get(sym, [])
            if not buf:
                return 0
            return int(buf[-1].get("seq") or 0)

    def get_latest_agg_trade_event_seq(self, symbol: str) -> int:
        sym = str(symbol).upper()
        with self._lock:
            buf = self.agg_trade_events.get(sym, [])
            if not buf:
                return 0
            return int(buf[-1].get("seq") or 0)

    def drain_private_events(self, symbol: str, since_seq: int = 0) -> Dict[str, Any]:
        sym = str(symbol).upper()
        with self._lock:
            buf = list(self.private_events_by_symbol.get(sym, []))
        return self._drain_event_buffer(buf=buf, since_seq=since_seq)

    def drain_agg_trade_events(self, symbol: str, since_seq: int = 0) -> Dict[str, Any]:
        sym = str(symbol).upper()
        with self._lock:
            buf = list(self.agg_trade_events.get(sym, []))
        return self._drain_event_buffer(buf=buf, since_seq=since_seq)

    @staticmethod
    def _drain_event_buffer(buf: List[Dict[str, Any]], since_seq: int) -> Dict[str, Any]:
        if not buf:
            return {"events": [], "next_seq": int(since_seq), "truncated": False}

        since_seq_int = max(0, int(since_seq))
        first_seq = int(buf[0].get("seq") or 0)
        truncated = since_seq_int < max(0, first_seq - 1)
        if truncated:
            events = buf
        else:
            events = [row for row in buf if int(row.get("seq") or 0) > since_seq_int]
        next_seq = int(events[-1].get("seq") or since_seq_int) if events else int(since_seq_int)
        return {
            "events": events,
            "next_seq": next_seq,
            "truncated": bool(truncated),
        }

    def get_order_updates(self, symbol: str, lookback_seconds: int = 1) -> List[Dict[str, Any]]:
        cutoff_ms = _now_ms() - max(1, int(lookback_seconds)) * 1000
        sym = str(symbol).upper()
        with self._lock:
            rows = self.recent_order_updates.get(sym, [])
            out = [asdict(x) for x in rows if x.event_time_ms >= cutoff_ms]
        return out

    def detect_filled_exit_from_ws(
        self,
        symbol: str,
        order_ids_by_reason: Dict[str, Optional[int]],
    ) -> Dict[str, Any]:
        sym = str(symbol).upper()
        candidates: List[Dict[str, Any]] = []
        with self._lock:
            for reason, oid in (order_ids_by_reason or {}).items():
                if oid is None:
                    continue
                upd = self.user_orders_by_id.get(int(oid))
                if upd is None or upd.symbol.upper() != sym:
                    continue
                status = str(upd.status or "").upper()
                cum = float(upd.cum_filled_qty or 0.0)
                if status == "FILLED" and cum > 0:
                    candidates.append(
                        {
                            "reason": str(reason),
                            "order_id": int(oid),
                            "filled_qty": cum,
                            "avg_price": upd.avg_price,
                            "update_time_ms": int(upd.event_time_ms),
                            "status": status,
                            "raw": asdict(upd),
                        }
                    )

        if not candidates:
            return {
                "reason": "UNKNOWN",
                "order_id": None,
                "filled_qty": 0.0,
                "avg_price": None,
                "update_time_ms": None,
                "status": "",
                "raw": {},
            }

        candidates.sort(key=lambda x: int(x.get("update_time_ms") or 0))
        return candidates[0]

    def get_position_amt(
        self,
        symbol: str,
        *,
        prefer_ws: bool = True,
        fallback_rest: bool = True,
    ) -> Optional[float]:
        sym = str(symbol).upper()
        now_ms = _now_ms()

        with self._lock:
            healthy = self._is_user_stream_healthy_unlocked(now_ms=now_ms)
            ws_pos = self.user_positions.get(sym)
            ws_seeded = self._ws_account_seeded_from_rest or (self.user_stream_last_account_event_ms > 0)

        if prefer_ws and healthy:
            if ws_pos is not None:
                return float(ws_pos.position_amt)
            if ws_seeded:
                return 0.0

        if fallback_rest and self._has_auth:
            self.reconcile_account_state_from_rest(force=False, merge_into_ws=False)
            with self._lock:
                if sym in self._rest_positions_by_symbol:
                    return float(self._rest_positions_by_symbol[sym])

        if ws_pos is not None:
            return float(ws_pos.position_amt)
        return None

    def get_position_abs_qty(
        self,
        symbol: str,
        *,
        prefer_ws: bool = True,
        fallback_rest: bool = True,
    ) -> Optional[float]:
        amt = self.get_position_amt(symbol, prefer_ws=prefer_ws, fallback_rest=fallback_rest)
        if amt is None:
            return None
        return abs(float(amt))

    def get_total_margin_balance(
        self,
        *,
        prefer_ws: bool = True,
        fallback_rest: bool = True,
    ) -> Optional[float]:
        now_ms = _now_ms()
        with self._lock:
            healthy = self._is_user_stream_healthy_unlocked(now_ms=now_ms)
            usdt = self.user_balances.get("USDT")
            wb = _to_float((usdt or {}).get("wallet_balance")) if isinstance(usdt, dict) else None
            cw = _to_float((usdt or {}).get("cross_wallet_balance")) if isinstance(usdt, dict) else None
        if prefer_ws and healthy:
            if wb is not None and wb > 0:
                return float(wb)
            if cw is not None and cw > 0:
                return float(cw)

        if fallback_rest and self._has_auth:
            self.reconcile_account_state_from_rest(force=False, merge_into_ws=False)
            with self._lock:
                mb = self._rest_total_margin_balance
                if mb is not None and mb > 0:
                    return float(mb)

        if wb is not None:
            return float(wb)
        if cw is not None:
            return float(cw)
        return None

    def _build_account_state_unlocked(self, symbol: str, now_ms: int, fallback_rest: bool) -> Dict[str, Any]:
        sym = str(symbol).upper()
        healthy = self._is_user_stream_healthy_unlocked(now_ms=now_ms)
        ws_pos = self.user_positions.get(sym)
        ws_seeded = self._ws_account_seeded_from_rest or (self.user_stream_last_account_event_ms > 0)

        amt: Optional[float] = None
        source = "UNKNOWN"
        entry_px = None
        last_update_ms = None
        if healthy:
            if ws_pos is not None:
                amt = float(ws_pos.position_amt)
                entry_px = ws_pos.entry_price
                last_update_ms = int(ws_pos.event_time_ms)
                source = "WS"
            elif ws_seeded:
                amt = 0.0
                source = "WS"

        if amt is None and fallback_rest and self._has_auth:
            amt = float(self._rest_positions_by_symbol.get(sym, 0.0))
            source = "REST_FALLBACK"
            last_update_ms = int(self._rest_last_reconcile_ms) if self._rest_last_reconcile_ms > 0 else None

        if amt is None and ws_pos is not None:
            amt = float(ws_pos.position_amt)
            entry_px = ws_pos.entry_price
            last_update_ms = int(ws_pos.event_time_ms)
            source = "WS_STALE"

        return {
            "symbol": sym,
            "position_amt": amt,
            "position_abs_qty": (abs(float(amt)) if amt is not None else None),
            "entry_price": entry_px,
            "source": source,
            "last_update_ms": last_update_ms,
            "user_stream_healthy": bool(healthy),
            "user_stream_last_event_ms": self.user_stream_last_event_ms or None,
            "user_stream_last_order_event_ms": self.user_stream_last_order_event_ms or None,
            "user_stream_last_account_event_ms": self.user_stream_last_account_event_ms or None,
        }
