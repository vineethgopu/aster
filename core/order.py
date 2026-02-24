# order.py
from __future__ import annotations

import time
import logging
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

from aster.rest_api import Client as AsterRestClient
from aster.error import ClientError


# ----------------------------
# Helpers
# ----------------------------

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _bps_ret(px: float, ref: float) -> float:
    """Return in bps: 1e4 * (px/ref - 1)."""
    if ref == 0:
        return 0.0
    return 1e4 * (px / ref - 1.0)


# ----------------------------
# Data models
# ----------------------------

@dataclass
class EntryResult:
    ok: bool
    symbol: str
    side: str                    # BUY/SELL (entry side)
    requested_qty: float
    filled_qty: float
    vwap_fill_px: Optional[float]
    maker_order_id: Optional[int]        # always None in taker-only mode
    taker_order_id: Optional[int]
    notes: str
    raw: Dict[str, Any]


@dataclass
class PositionState:
    symbol: str
    side: str                    # entry side: BUY means long, SELL means short
    qty: float                   # filled quantity (abs)
    entry_vwap_px: float
    opened_time_ms: int
    maker_order_id: Optional[int] = None  # always None in taker-only mode
    taker_order_id: Optional[int] = None
    take_profit_order_id: Optional[int] = None
    stop_loss_order_id: Optional[int] = None
    trailing_stop_order_id: Optional[int] = None

    @property
    def is_long(self) -> bool:
        return self.side == "BUY"

    @property
    def is_short(self) -> bool:
        return self.side == "SELL"


@dataclass
class ExitResult:
    ok: bool
    symbol: str
    reason: str                  # TAKE_PROFIT / STOP_LOSS / MARGIN_KILL / MANUAL / ERROR
    closed_qty: float
    close_vwap_px: Optional[float]
    close_order_id: Optional[int]
    notes: str
    raw: Dict[str, Any]


# ----------------------------
# Order placer
# ----------------------------

class OrderPlacer:
    """
    TAKER-ONLY EXECUTION

    Entry:
      - Single IOC LIMIT that crosses touch

    Exit:
      - TAKE_PROFIT_MARKET trigger (CONTRACT_PRICE) computed from entry_vwap + c1_bps
      - STOP_MARKET trigger (MARK_PRICE) computed from entry_vwap - c2_bps
      - MARGIN_KILL:
          safety_multiple = totalMarginBalance / totalMaintMargin
          Trigger kill if safety_multiple <= 1.2 (default).
      - Kill close uses reduce-only MARKET (taker-style)
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.asterdex.com",
        recv_window_ms: int = 2000,
        poll_interval_s: float = 0.25,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.rest = AsterRestClient(api_key, api_secret, base_url=base_url)
        self.recv_window_ms = recv_window_ms
        self.poll_interval_s = poll_interval_s
        self.log = logger or logging.getLogger(__name__)
        self._symbol_filters: Dict[str, Dict[str, float]] = {}

    def _load_exchange_filters(self) -> None:
        if self._symbol_filters:
            return
        info = self.rest.exchange_info()
        data = info.get("data") if isinstance(info, dict) and isinstance(info.get("data"), dict) else info
        symbols = data.get("symbols") if isinstance(data, dict) else None
        if not isinstance(symbols, list):
            raise RuntimeError(f"Unexpected exchange_info format: {info}")

        for s in symbols:
            if not isinstance(s, dict):
                continue
            sym = str(s.get("symbol", "")).upper()
            if not sym:
                continue
            f_out: Dict[str, float] = {}
            filters = s.get("filters")
            if isinstance(filters, list):
                for f in filters:
                    if not isinstance(f, dict):
                        continue
                    ftype = str(f.get("filterType", ""))
                    if ftype == "PRICE_FILTER":
                        tick = _safe_float(f.get("tickSize"))
                        if tick and tick > 0:
                            f_out["tickSize"] = tick
                    elif ftype == "LOT_SIZE":
                        step = _safe_float(f.get("stepSize"))
                        min_qty = _safe_float(f.get("minQty"))
                        max_qty = _safe_float(f.get("maxQty"))
                        if step and step > 0:
                            f_out["stepSize"] = step
                        if min_qty is not None:
                            f_out["minQty"] = min_qty
                        if max_qty is not None:
                            f_out["maxQty"] = max_qty
                    elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
                        min_notional = _safe_float(f.get("notional", f.get("minNotional")))
                        if min_notional is not None:
                            f_out["minNotional"] = min_notional
            self._symbol_filters[sym] = f_out

    def _get_symbol_filters(self, symbol: str) -> Dict[str, float]:
        self._load_exchange_filters()
        out = self._symbol_filters.get(symbol.upper())
        if out is None:
            raise ValueError(f"No symbol filters found for {symbol}")
        return out

    @staticmethod
    def _round_to_step(value: float, step: float, mode: str) -> float:
        if step <= 0:
            return float(value)
        d_val = Decimal(str(value))
        d_step = Decimal(str(step))
        q = d_val / d_step
        rounded = q.to_integral_value(rounding=ROUND_UP if mode == "up" else ROUND_DOWN) * d_step
        return float(rounded)

    def _round_price(self, symbol: str, price: float, mode: str = "down") -> float:
        tick = self._get_symbol_filters(symbol).get("tickSize")
        if tick is None or tick <= 0:
            return float(price)
        return self._round_to_step(price, tick, mode=mode)

    def _round_qty(self, symbol: str, qty: float, mode: str = "down") -> float:
        step = self._get_symbol_filters(symbol).get("stepSize")
        if step is None or step <= 0:
            return float(qty)
        return self._round_to_step(qty, step, mode=mode)

    def get_entry_limit_price(
        self,
        symbol: str,
        side: str,
        price_source: Any,
    ) -> Optional[float]:
        bid_px, ask_px = self._get_touch(symbol, price_source)
        if bid_px is None or ask_px is None:
            return None
        side = side.upper().strip()
        raw_price = ask_px if side == "BUY" else bid_px
        # For taker LIMIT crossing, bias BUY up and SELL down to preserve aggressiveness.
        mode = "up" if side == "BUY" else "down"
        return self._round_price(symbol, raw_price, mode=mode)

    def compute_qty_for_notional(
        self,
        symbol: str,
        entry_price: float,
        order_notional_usd: float,
    ) -> float:
        if entry_price <= 0:
            raise ValueError("entry_price must be > 0")
        if order_notional_usd <= 0:
            raise ValueError("order_notional_usd must be > 0")

        filters = self._get_symbol_filters(symbol)
        raw_qty = order_notional_usd / entry_price
        qty = self._round_qty(symbol, raw_qty, mode="up")

        min_qty = filters.get("minQty")
        if min_qty is not None and qty < min_qty:
            qty = self._round_qty(symbol, min_qty, mode="up")

        min_notional = filters.get("minNotional")
        if min_notional is not None and qty * entry_price < min_notional:
            qty = self._round_qty(symbol, min_notional / entry_price, mode="up")

        max_qty = filters.get("maxQty")
        if max_qty is not None and qty > max_qty:
            raise ValueError(f"Computed qty {qty} exceeds maxQty {max_qty} for {symbol}")
        return float(qty)

    # ----------------------------
    # Public API: Entry (TAKER-ONLY)
    # ----------------------------

    def entry(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price_source: Any,
        taker_extra: Optional[Dict[str, Any]] = None,
        take_profit_bps: Optional[float] = None,
        stop_loss_bps: Optional[float] = None,
        trailing_activation_bps: Optional[float] = None,
        trailing_activation_price: Optional[float] = None,
        trailing_callback_rate: Optional[float] = None,
        exit_trigger_extra: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[PositionState], EntryResult]:
        """
        TAKer-only entry:
          - Single IOC LIMIT crossing touch.
          - BUY: ask
          - SELL: bid

        Returns (PositionState or None, EntryResult).
        """
        side = side.upper().strip()
        if side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")

        taker_extra = taker_extra or {}

        bid_px, ask_px = self._get_touch(symbol, price_source)
        if bid_px is None or ask_px is None:
            res = EntryResult(
                ok=False, symbol=symbol, side=side, requested_qty=quantity,
                filled_qty=0.0, vwap_fill_px=None,
                maker_order_id=None, taker_order_id=None,
                notes="No BBO available for entry.",
                raw={"bid_px": bid_px, "ask_px": ask_px},
            )
            return None, res

        if side == "BUY":
            taker_price = self._round_price(symbol, ask_px, mode="up")
        else:
            taker_price = self._round_price(symbol, bid_px, mode="down")

        quantity = self._round_qty(symbol, quantity, mode="down")

        self.log.info(
            f"[ENTRY] taker-only submit {symbol} {side} qty={quantity} px={taker_price} tif=IOC"
        )

        try:
            taker_resp = self._new_order(
                symbol=symbol,
                side=side,
                type_="LIMIT",
                quantity=quantity,
                time_in_force="IOC",
                price=taker_price,
                extra=taker_extra,
            )
        except ClientError as e:
            res = EntryResult(
                ok=False, symbol=symbol, side=side, requested_qty=quantity,
                filled_qty=0.0, vwap_fill_px=None,
                maker_order_id=None, taker_order_id=None,
                notes=f"Taker IOC rejected: {getattr(e,'error_message',str(e))}",
                raw={"taker_error": str(e)},
            )
            return None, res

        taker_order_id = self._extract_order_id(taker_resp)

        taker_filled_qty = 0.0
        taker_avg_px = None
        taker_q = None
        if taker_order_id is not None:
            try:
                taker_q = self._query_order(symbol, taker_order_id)
                _, taker_filled_qty, taker_avg_px = self._parse_query(taker_q)
            except ClientError:
                pass

        if (taker_filled_qty or 0.0) <= 0 or taker_avg_px is None or taker_avg_px <= 0:
            res = EntryResult(
                ok=False, symbol=symbol, side=side, requested_qty=quantity,
                filled_qty=taker_filled_qty or 0.0, vwap_fill_px=taker_avg_px,
                maker_order_id=None, taker_order_id=taker_order_id,
                notes="Entry failed (no fills).",
                raw={"taker_new": taker_resp, "taker_query": taker_q},
            )
            return None, res

        pos = PositionState(
            symbol=symbol,
            side=side,
            qty=taker_filled_qty,
            entry_vwap_px=taker_avg_px,
            opened_time_ms=_now_ms(),
            maker_order_id=None,
            taker_order_id=taker_order_id,
        )

        # Optional: place exchange-native trigger exits immediately after entry fill.
        if (
            take_profit_bps is not None
            and stop_loss_bps is not None
            and take_profit_bps > 0
            and stop_loss_bps > 0
        ):
            trigger_resp = self.place_exit_triggers(
                pos=pos,
                take_profit_bps=take_profit_bps,
                stop_loss_bps=stop_loss_bps,
                trailing_activation_bps=trailing_activation_bps,
                trailing_activation_price=trailing_activation_price,
                trailing_callback_rate=trailing_callback_rate,
                extra=(exit_trigger_extra or {}),
            )
            pos.take_profit_order_id = trigger_resp.get("take_profit_order_id")
            pos.stop_loss_order_id = trigger_resp.get("stop_loss_order_id")
            pos.trailing_stop_order_id = trigger_resp.get("trailing_stop_order_id")
        else:
            trigger_resp = {}

        res = EntryResult(
            ok=True, symbol=symbol, side=side, requested_qty=quantity,
            filled_qty=pos.qty, vwap_fill_px=pos.entry_vwap_px,
            maker_order_id=None, taker_order_id=taker_order_id,
            notes="Taker-only IOC filled.",
            raw={
                "taker_new": taker_resp,
                "taker_query": taker_q,
                "taker_limit_price": taker_price,
                "take_profit_order_id": pos.take_profit_order_id,
                "stop_loss_order_id": pos.stop_loss_order_id,
                "trailing_stop_order_id": pos.trailing_stop_order_id,
                "take_profit_price": trigger_resp.get("take_profit_stop_price"),
                "stop_loss_mark_price": trigger_resp.get("stop_loss_stop_price"),
                "trailing_activation_price": trigger_resp.get("trailing_activation_price"),
                "trailing_callback_rate": trigger_resp.get("trailing_callback_rate"),
            },
        )
        return pos, res

    def _calc_exit_trigger_prices(
        self,
        pos: PositionState,
        take_profit_bps: float,
        stop_loss_bps: float,
    ) -> Tuple[float, float]:
        entry = pos.entry_vwap_px
        tp_frac = take_profit_bps / 1e4
        sl_frac = stop_loss_bps / 1e4
        if pos.is_long:
            tp_stop_price = entry * (1.0 + tp_frac)
            sl_stop_price = entry * (1.0 - sl_frac)
        else:
            tp_stop_price = entry * (1.0 - tp_frac)
            sl_stop_price = entry * (1.0 + sl_frac)
        return tp_stop_price, sl_stop_price

    def place_exit_triggers(
        self,
        pos: PositionState,
        take_profit_bps: float,
        stop_loss_bps: float,
        trailing_activation_bps: Optional[float] = None,
        trailing_activation_price: Optional[float] = None,
        trailing_callback_rate: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Places reduce-only exit triggers:
          - TAKE_PROFIT_MARKET (CONTRACT_PRICE)
          - STOP_MARKET (MARK_PRICE)
          - TRAILING_STOP_MARKET
        """
        if take_profit_bps <= 0 or stop_loss_bps <= 0:
            raise ValueError("take_profit_bps and stop_loss_bps must be > 0")

        tp_stop_price, sl_stop_price = self._calc_exit_trigger_prices(
            pos=pos,
            take_profit_bps=take_profit_bps,
            stop_loss_bps=stop_loss_bps,
        )
        tp_stop_price = self._round_price(pos.symbol, tp_stop_price, mode="down")
        sl_stop_price = self._round_price(pos.symbol, sl_stop_price, mode="down")
        close_side = "SELL" if pos.is_long else "BUY"
        entry_px = pos.entry_vwap_px
        if trailing_activation_price is None:
            if trailing_activation_bps is None or trailing_activation_bps <= 0:
                # Default to halfway-to-TP in bps terms if explicit activation is not provided.
                trailing_activation_bps = take_profit_bps * 0.5
            act_frac = trailing_activation_bps / 1e4
            if pos.is_long:
                trailing_activation_price = entry_px * (1.0 + act_frac)
            else:
                trailing_activation_price = entry_px * (1.0 - act_frac)
        trailing_activation_price = self._round_price(pos.symbol, trailing_activation_price, mode="down")
        if trailing_callback_rate is None:
            # As requested: (activation - execution) / execution (absolute).
            trailing_callback_rate = abs((trailing_activation_price - entry_px) / entry_px)
        if trailing_callback_rate <= 0:
            trailing_callback_rate = 0.0001

        common_extra = dict(extra or {})
        common_extra.setdefault("reduceOnly", True)
        common_extra.setdefault("priceProtect", "TRUE")

        tp_extra = dict(common_extra)
        tp_extra.setdefault("workingType", "CONTRACT_PRICE")

        sl_extra = dict(common_extra)
        sl_extra.setdefault("workingType", "MARK_PRICE")
        tsl_extra = dict(common_extra)

        self.log.info(
            (
                f"[EXIT_ARM] {pos.symbol} side={close_side} qty={pos.qty} "
                f"tp_stop={tp_stop_price} (CONTRACT_PRICE), "
                f"sl_stop={sl_stop_price} (MARK_PRICE), "
                f"trail_activation={trailing_activation_price}, trail_callback={trailing_callback_rate}"
            )
        )

        tp_resp = self._new_order(
            symbol=pos.symbol,
            side=close_side,
            type_="TAKE_PROFIT_MARKET",
            quantity=pos.qty,
            stop_price=tp_stop_price,
            extra=tp_extra,
        )
        sl_resp = self._new_order(
            symbol=pos.symbol,
            side=close_side,
            type_="STOP_MARKET",
            quantity=pos.qty,
            stop_price=sl_stop_price,
            extra=sl_extra,
        )
        tsl_resp = self._new_order(
            symbol=pos.symbol,
            side=close_side,
            type_="TRAILING_STOP_MARKET",
            quantity=pos.qty,
            extra={
                **tsl_extra,
                "activationPrice": trailing_activation_price,
                "callbackRate": trailing_callback_rate,
            },
        )

        return {
            "take_profit_order_id": self._extract_order_id(tp_resp),
            "stop_loss_order_id": self._extract_order_id(sl_resp),
            "trailing_stop_order_id": self._extract_order_id(tsl_resp),
            "take_profit_stop_price": tp_stop_price,
            "stop_loss_stop_price": sl_stop_price,
            "trailing_activation_price": trailing_activation_price,
            "trailing_callback_rate": trailing_callback_rate,
            "tp_raw": tp_resp,
            "sl_raw": sl_resp,
            "tsl_raw": tsl_resp,
        }

    # ----------------------------
    # Public API: Exit (monitor + close)
    # ----------------------------

    def maybe_exit(
        self,
        pos: PositionState,
        price_source: Any,
        c1_bps: float,
        c2_bps: float,
        margin_safety_multiple_min: float = 1.2,
        account_poll: bool = True,
        close_extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[ExitResult]:
        """
        Returns ExitResult if a margin-kill exit was triggered and a close order was submitted.
        Returns None if no exit is needed.

        MARGIN_KILL:
          - safety_multiple = totalMarginBalance / totalMaintMargin
          - Trigger if safety_multiple <= margin_safety_multiple_min (default 1.2)
        """
        close_extra = close_extra or {}

        # Margin kill-switch
        if account_poll:
            safety_multiple = self._get_margin_safety_multiple_total_usdt()
            if safety_multiple is not None and safety_multiple > 0:
                if safety_multiple <= margin_safety_multiple_min:
                    return self._close_position(
                        pos=pos,
                        price_source=price_source,
                        reason="MARGIN_KILL",
                        notes=(
                            f"safety_multiple={safety_multiple:.4f} <= {margin_safety_multiple_min:.4f}"
                        ),
                        extra=close_extra,
                        order_type="MARKET",
                    )

        return None

    def close_position(
        self,
        pos: PositionState,
        price_source: Any,
        reason: str = "MANUAL",
        notes: str = "",
        extra: Optional[Dict[str, Any]] = None,
        order_type: str = "MARKET",
        refresh_from_exchange: bool = True,
    ) -> ExitResult:
        """
        Force-close a position with a reduce-only taker order.

        If refresh_from_exchange is True, uses live exchange positionAmt (sign+qty) to
        select the close side and quantity. This is safer than relying on local state.
        """
        extra = extra or {}

        pos_to_close = pos
        if refresh_from_exchange:
            live_amt = self.get_position_amt(pos.symbol)
            if live_amt is not None and abs(live_amt) > 0:
                # Derive side from exchange sign to avoid accidental wrong-way closes.
                live_side = "BUY" if live_amt > 0 else "SELL"
                pos_to_close = PositionState(
                    symbol=pos.symbol,
                    side=live_side,
                    qty=abs(live_amt),
                    entry_vwap_px=pos.entry_vwap_px or 0.0,
                    opened_time_ms=pos.opened_time_ms,
                    maker_order_id=pos.maker_order_id,
                    taker_order_id=pos.taker_order_id,
                    take_profit_order_id=pos.take_profit_order_id,
                    stop_loss_order_id=pos.stop_loss_order_id,
                    trailing_stop_order_id=pos.trailing_stop_order_id,
                )

        return self._close_position(
            pos=pos_to_close,
            price_source=price_source,
            reason=reason,
            notes=notes,
            extra=extra,
            order_type=order_type,
        )

    # ----------------------------
    # Internal: close position
    # ----------------------------

    def _close_position(
        self,
        pos: PositionState,
        price_source: Any,
        reason: str,
        notes: str,
        extra: Dict[str, Any],
        order_type: str = "MARKET",
    ) -> ExitResult:
        """Close with reduce-only taker order."""
        close_side = "SELL" if pos.is_long else "BUY"

        payload_extra = dict(extra)
        payload_extra.setdefault("reduceOnly", True)

        try:
            if order_type.upper() == "MARKET":
                self.log.info(
                    f"[EXIT] {reason} submit {pos.symbol} {close_side} qty={pos.qty} type=MARKET reduceOnly=True"
                )
                close_resp = self._new_order(
                    symbol=pos.symbol,
                    side=close_side,
                    type_="MARKET",
                    quantity=pos.qty,
                    extra=payload_extra,
                )
            else:
                bid_px, ask_px = self._get_touch(pos.symbol, price_source)
                if bid_px is None or ask_px is None:
                    return ExitResult(
                        ok=False, symbol=pos.symbol, reason="ERROR",
                        closed_qty=0.0, close_vwap_px=None, close_order_id=None,
                        notes=f"{reason} triggered but no BBO available to close.",
                        raw={},
                    )
                close_price = bid_px if close_side == "SELL" else ask_px
                close_price = self._round_price(pos.symbol, close_price, mode=("down" if close_side == "SELL" else "up"))
                self.log.info(
                    (
                        f"[EXIT] {reason} submit {pos.symbol} {close_side} qty={pos.qty} "
                        f"type=LIMIT px={close_price} tif=IOC reduceOnly=True"
                    )
                )
                close_resp = self._new_order(
                    symbol=pos.symbol,
                    side=close_side,
                    type_="LIMIT",
                    quantity=pos.qty,
                    time_in_force="IOC",
                    price=close_price,
                    extra=payload_extra,
                )
        except ClientError as e:
            return ExitResult(
                ok=False, symbol=pos.symbol, reason="ERROR",
                closed_qty=0.0, close_vwap_px=None, close_order_id=None,
                notes=f"Close order rejected: {getattr(e,'error_message',str(e))}",
                raw={"error": str(e)},
            )

        close_order_id = self._extract_order_id(close_resp)

        filled_qty = 0.0
        avg_px = None
        close_q = None
        if close_order_id is not None:
            try:
                close_q = self._query_order(pos.symbol, close_order_id)
                _, filled_qty, avg_px = self._parse_query(close_q)
            except ClientError:
                pass

        return ExitResult(
            ok=(filled_qty > 0),
            symbol=pos.symbol,
            reason=reason,
            closed_qty=filled_qty,
            close_vwap_px=avg_px if (avg_px is not None and avg_px > 0) else None,
            close_order_id=close_order_id,
            notes=notes,
            raw={"close_new": close_resp, "close_query": close_q},
        )

    # ----------------------------
    # Internal: REST wrappers
    # ----------------------------

    def _new_order(
        self,
        symbol: str,
        side: str,
        type_: str,
        quantity: Optional[float] = None,
        time_in_force: Optional[str] = None,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = dict(extra or {})
        payload.update({"symbol": symbol, "side": side, "type": type_, "recvWindow": self.recv_window_ms})
        if quantity is not None:
            payload["quantity"] = self._round_qty(symbol, quantity, mode="down")
        if time_in_force is not None:
            payload["timeInForce"] = time_in_force
        if price is not None:
            payload["price"] = self._round_price(symbol, price, mode=("up" if side.upper() == "BUY" else "down"))
        if stop_price is not None:
            payload["stopPrice"] = self._round_price(symbol, stop_price, mode="down")
        if "activationPrice" in payload and payload["activationPrice"] is not None:
            payload["activationPrice"] = self._round_price(symbol, float(payload["activationPrice"]), mode="down")
        return self.rest.new_order(**payload)

    def query_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if order_id is None and not orig_client_order_id:
            raise ValueError("Either order_id or orig_client_order_id must be provided")

        payload: Dict[str, Any] = {"symbol": symbol, "recvWindow": self.recv_window_ms}
        if order_id is not None:
            payload["orderId"] = int(order_id)
        if orig_client_order_id:
            payload["origClientOrderId"] = str(orig_client_order_id)
        return self.rest.sign_request("GET", "/fapi/v1/order", payload)

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if order_id is None and not orig_client_order_id:
            raise ValueError("Either order_id or orig_client_order_id must be provided")

        payload: Dict[str, Any] = {"symbol": symbol, "recvWindow": self.recv_window_ms}
        if order_id is not None:
            payload["orderId"] = int(order_id)
        if orig_client_order_id:
            payload["origClientOrderId"] = str(orig_client_order_id)
        return self.rest.sign_request("DELETE", "/fapi/v1/order", payload)

    def get_position_amt(self, symbol: str) -> Optional[float]:
        """Signed position amount (long > 0, short < 0)."""
        try:
            resp = self.rest.get_position_risk(symbol=symbol, recvWindow=max(self.recv_window_ms, 6000))
        except ClientError as e:
            self.log.warning(f"[POSITION] get_position_risk() failed: {getattr(e,'error_message',str(e))}")
            return None

        data = resp.get("data") if isinstance(resp, dict) and isinstance(resp.get("data"), (list, dict)) else resp
        rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = str(row.get("symbol", "")).upper()
            if sym != symbol.upper():
                continue
            amt = _safe_float(row.get("positionAmt"))
            if amt is not None:
                return float(amt)
        return None

    def get_position_abs_qty(self, symbol: str) -> Optional[float]:
        amt = self.get_position_amt(symbol)
        if amt is None:
            return None
        return abs(amt)

    def get_total_margin_balance(self) -> Optional[float]:
        """
        Returns account totalMarginBalance (USDT) when available.
        """
        try:
            acct = self.rest.account(recvWindow=max(self.recv_window_ms, 6000))
        except ClientError as e:
            self.log.warning(f"[ACCOUNT] account() failed: {getattr(e,'error_message',str(e))}")
            return None

        if not isinstance(acct, dict):
            return None
        d = acct.get("data") if isinstance(acct.get("data"), dict) else acct
        mb = _safe_float(d.get("totalMarginBalance"))
        if mb is None:
            return None
        return float(mb)

    def cancel_sibling_exit_orders(self, pos: PositionState) -> None:
        order_ids = [
            pos.take_profit_order_id,
            pos.stop_loss_order_id,
            pos.trailing_stop_order_id,
        ]
        for oid in order_ids:
            if oid is None:
                continue
            try:
                self.cancel_order(symbol=pos.symbol, order_id=oid)
                self.log.info(f"[CANCEL_SIBLING] {pos.symbol} order_id={oid} canceled")
            except Exception as e:
                # It's normal if sibling already triggered/canceled.
                self.log.info(f"[CANCEL_SIBLING] {pos.symbol} order_id={oid} skipped: {e}")

    def ensure_risk_setup(
        self,
        symbols: List[str],
        leverage: int = 10,
        margin_type: str = "ISOLATED",
    ) -> Dict[str, Dict[str, str]]:
        out: Dict[str, Dict[str, str]] = {}
        for sym in symbols:
            sym_u = str(sym).upper()
            out[sym_u] = {"leverage": "ok", "margin_type": "ok"}
            try:
                self.rest.change_leverage(symbol=sym_u, leverage=leverage, recvWindow=6000)
            except Exception as e:
                msg = str(e)
                out[sym_u]["leverage"] = msg
                self.log.info(f"[RISK_SETUP] {sym_u} leverage={leverage} failed/skipped: {msg}")
            try:
                self.rest.change_margin_type(symbol=sym_u, marginType=margin_type, recvWindow=6000)
            except Exception as e:
                msg = str(e)
                out[sym_u]["margin_type"] = msg
                self.log.info(f"[RISK_SETUP] {sym_u} marginType={margin_type} failed/skipped: {msg}")
        return out

    def _query_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return self.query_order(symbol=symbol, order_id=order_id)

    # ----------------------------
    # Internal: parsing
    # ----------------------------

    def _extract_order_id(self, resp: Any) -> Optional[int]:
        if isinstance(resp, dict) and "orderId" in resp:
            try:
                return int(resp["orderId"])
            except Exception:
                return None
        if isinstance(resp, dict) and isinstance(resp.get("data"), dict) and "orderId" in resp["data"]:
            try:
                return int(resp["data"]["orderId"])
            except Exception:
                return None
        return None

    def _parse_query(self, q: Any) -> Tuple[str, Optional[float], Optional[float]]:
        """
        Uses your schema:
          status, executedQty, avgPrice
        """
        if not isinstance(q, dict):
            return "", None, None
        d = q.get("data") if isinstance(q.get("data"), dict) else q
        status = str(d.get("status") or "")
        executed_qty = _safe_float(d.get("executedQty"))
        avg_px = _safe_float(d.get("avgPrice"))
        return status, executed_qty, avg_px

    # ----------------------------
    # Internal: market data reads (from your WS cache client)
    # ----------------------------

    def _get_touch(self, symbol: str, price_source: Any) -> Tuple[Optional[float], Optional[float]]:
        """
        Expects price_source.latest_bbo[symbol] with bid_px/ask_px (dict or object).
        """
        bbo = getattr(price_source, "latest_bbo", {}).get(symbol)
        if bbo is None:
            return None, None
        if isinstance(bbo, dict):
            return _safe_float(bbo.get("bid_px")), _safe_float(bbo.get("ask_px"))
        return _safe_float(getattr(bbo, "bid_px", None)), _safe_float(getattr(bbo, "ask_px", None))

    def _get_mark_price(self, symbol: str, price_source: Any) -> Optional[float]:
        """
        Expects price_source.latest_funding[symbol].mark_px (dict or object).
        """
        f = getattr(price_source, "latest_funding", {}).get(symbol)
        if f is None:
            return None
        if isinstance(f, dict):
            return _safe_float(f.get("mark_px"))
        return _safe_float(getattr(f, "mark_px", None))

    def _get_last_price(self, symbol: str, price_source: Any) -> Optional[float]:
        """
        Prefer latest aggTrade price from price_source.recent_agg_trades[symbol] (list),
        else fallback to mid.
        """
        buf: List[Any] = getattr(price_source, "recent_agg_trades", {}).get(symbol) or []
        last_px = None
        last_t = -1
        for t in buf[-200:]:
            tt = getattr(t, "trade_time_ms", None)
            px = getattr(t, "price", None)
            if tt is None or px is None:
                continue
            if tt > last_t:
                last_t = tt
                last_px = float(px)

        if last_px is not None:
            return last_px

        bid_px, ask_px = self._get_touch(symbol, price_source)
        if bid_px is None or ask_px is None:
            return None
        return 0.5 * (bid_px + ask_px)

    # ----------------------------
    # Internal: margin / liquidation safety
    # ----------------------------

    def _get_margin_safety_multiple_total_usdt(self) -> Optional[float]:
        """
        From your account() schema:
          totalMaintMargin, totalMarginBalance

        Define:
          safety_multiple = totalMarginBalance / totalMaintMargin
        """
        try:
            acct = self.rest.account(recvWindow=max(self.recv_window_ms, 6000))
        except ClientError as e:
            self.log.warning(f"[MARGIN] account() failed: {getattr(e,'error_message',str(e))}")
            return None

        if not isinstance(acct, dict):
            return None
        d = acct.get("data") if isinstance(acct.get("data"), dict) else acct

        mb = _safe_float(d.get("totalMarginBalance"))
        mm = _safe_float(d.get("totalMaintMargin"))

        if mb is None or mm is None:
            return None
        if mb <= 0:
            return None
        if mm <= 0:
            # No positions / no maint requirement -> ratio not meaningful
            return None

        return mb / mm
