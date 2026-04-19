from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from .client_types import (
    AGG_TRADE_EVENT_BUFFER_TRIM,
    BBO,
    DERIVED_BAR_MINS,
    FundingInfo,
    AggTrade,
    KLINE_INTERVAL_1M,
    Kline1m,
    L2_LEVELS,
    L2Depth,
    _now_ms,
    _to_float,
    _to_int,
)


class MarketDataMixin:
    def rest_snapshot(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"ts_ms": _now_ms(), "symbols": {}}
        for sym in self.symbols:
            s: Dict[str, Any] = {}
            s["klines_1m"] = self.rest.klines(sym, KLINE_INTERVAL_1M, limit=20)
            s["bookTicker"] = self.rest.book_ticker(sym)
            s["markPrice"] = self.rest.mark_price(sym)
            s["aggTrades"] = self.rest.agg_trades(sym, limit=100)
            s["depth5"] = self.rest.depth(sym, limit=L2_LEVELS)
            out["symbols"][sym] = s
        return out

    def _seed_from_rest_snapshot(self, startup: Dict[str, Any]) -> None:
        ts_ms = _to_int(startup.get("ts_ms")) or _now_ms()
        symbols_data = startup.get("symbols") or {}

        with self._lock:
            for sym in self.symbols:
                payload = symbols_data.get(sym) or {}

                bt = payload.get("bookTicker")
                if isinstance(bt, dict):
                    bid_px = _to_float(bt.get("bidPrice", bt.get("b")))
                    bid_qty = _to_float(bt.get("bidQty", bt.get("B")))
                    ask_px = _to_float(bt.get("askPrice", bt.get("a")))
                    ask_qty = _to_float(bt.get("askQty", bt.get("A")))
                    if None not in (bid_px, bid_qty, ask_px, ask_qty):
                        ev_ms = _to_int(bt.get("time", bt.get("E"))) or ts_ms
                        self.latest_bbo[sym] = BBO(
                            symbol=sym,
                            event_time_ms=ev_ms,
                            bid_px=bid_px,
                            bid_qty=bid_qty,
                            ask_px=ask_px,
                            ask_qty=ask_qty,
                        )

                mp = payload.get("markPrice")
                if isinstance(mp, dict):
                    mark_px = _to_float(mp.get("markPrice", mp.get("p")))
                    index_px = _to_float(mp.get("indexPrice", mp.get("i")))
                    funding_rate = _to_float(mp.get("lastFundingRate", mp.get("r")))
                    next_ft = _to_int(mp.get("nextFundingTime", mp.get("T")))
                    if None not in (mark_px, index_px, funding_rate, next_ft):
                        ev_ms = _to_int(mp.get("time", mp.get("E"))) or ts_ms
                        self.latest_funding[sym] = FundingInfo(
                            symbol=sym,
                            event_time_ms=ev_ms,
                            mark_px=mark_px,
                            index_px=index_px,
                            funding_rate=funding_rate,
                            next_funding_time_ms=next_ft,
                        )

                klines = payload.get("klines_1m")
                if isinstance(klines, list) and klines:
                    k_last = klines[-1]
                    if isinstance(k_last, (list, tuple)) and len(k_last) >= 12:
                        try:
                            self.latest_kline_1m[sym] = Kline1m(
                                symbol=sym,
                                event_time_ms=ts_ms,
                                start_time_ms=int(k_last[0]),
                                close_time_ms=int(k_last[6]),
                                interval=KLINE_INTERVAL_1M,
                                open=float(k_last[1]),
                                high=float(k_last[2]),
                                low=float(k_last[3]),
                                close=float(k_last[4]),
                                base_vol=float(k_last[5]),
                                quote_vol=float(k_last[7]),
                                num_trades=int(k_last[8]),
                                is_closed=bool(k_last[11]),
                            )
                        except Exception:
                            pass
                    elif isinstance(k_last, dict):
                        try:
                            self.latest_kline_1m[sym] = Kline1m(
                                symbol=sym,
                                event_time_ms=ts_ms,
                                start_time_ms=int(k_last.get("openTime", k_last.get("t"))),
                                close_time_ms=int(k_last.get("closeTime", k_last.get("T"))),
                                interval=str(k_last.get("interval", k_last.get("i", KLINE_INTERVAL_1M))),
                                open=float(k_last.get("open", k_last.get("o"))),
                                high=float(k_last.get("high", k_last.get("h"))),
                                low=float(k_last.get("low", k_last.get("l"))),
                                close=float(k_last.get("close", k_last.get("c"))),
                                base_vol=float(k_last.get("volume", k_last.get("v"))),
                                quote_vol=float(k_last.get("quoteAssetVolume", k_last.get("q", 0.0))),
                                num_trades=int(k_last.get("numTrades", k_last.get("n", 0))),
                                is_closed=bool(k_last.get("isClosed", k_last.get("x", True))),
                            )
                        except Exception:
                            pass

                trades = payload.get("aggTrades")
                if isinstance(trades, list):
                    parsed_trades: List[AggTrade] = []
                    for t in trades:
                        if not isinstance(t, dict):
                            continue
                        agg_id = _to_int(t.get("a", t.get("aggId")))
                        price = _to_float(t.get("p", t.get("price")))
                        qty = _to_float(t.get("q", t.get("qty")))
                        maker_flag = t.get("m", t.get("isBuyerMaker"))
                        trade_time_ms = _to_int(t.get("T", t.get("tradeTime")))
                        event_time_ms = _to_int(t.get("E", t.get("eventTime"))) or trade_time_ms or ts_ms
                        if None in (agg_id, price, qty, trade_time_ms):
                            continue
                        parsed_trades.append(
                            AggTrade(
                                symbol=sym,
                                event_time_ms=int(event_time_ms),
                                trade_time_ms=int(trade_time_ms),
                                agg_id=int(agg_id),
                                price=price,
                                qty=qty,
                                is_buyer_maker=bool(maker_flag),
                            )
                        )
                    if parsed_trades:
                        parsed_trades.sort(key=lambda x: x.trade_time_ms)
                        self.recent_agg_trades[sym] = parsed_trades[-5000:]

                depth = payload.get("depth5")
                if isinstance(depth, dict):
                    bids_raw = depth.get("bids", depth.get("b", []))
                    asks_raw = depth.get("asks", depth.get("a", []))
                    bids: List[Tuple[float, float]] = []
                    asks: List[Tuple[float, float]] = []
                    for level in bids_raw[:L2_LEVELS]:
                        if isinstance(level, (list, tuple)) and len(level) >= 2:
                            px = _to_float(level[0])
                            qty = _to_float(level[1])
                            if None not in (px, qty):
                                bids.append((px, qty))
                    for level in asks_raw[:L2_LEVELS]:
                        if isinstance(level, (list, tuple)) and len(level) >= 2:
                            px = _to_float(level[0])
                            qty = _to_float(level[1])
                            if None not in (px, qty):
                                asks.append((px, qty))
                    ev_ms = _to_int(depth.get("E", depth.get("T"))) or ts_ms
                    if bids or asks:
                        self.latest_l2[sym] = L2Depth(
                            symbol=sym,
                            event_time_ms=ev_ms,
                            bids=bids,
                            asks=asks,
                        )

    def get_symbol_rows(
        self,
        *,
        lookback_seconds: int = 1,
        include_user_state: bool = False,
        fallback_rest_for_user_state: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        now_ms = _now_ms()
        if include_user_state and fallback_rest_for_user_state and self._has_auth:
            self.reconcile_account_state_from_rest(force=False, merge_into_ws=False)

        with self._lock:
            user_status = None
            if include_user_state:
                user_status = {
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
            out: Dict[str, Dict[str, Any]] = {}
            for sym in self.symbols:
                row: Dict[str, Any] = {
                    "bars": self.getBars(sym),
                    "bbo": self.getBBO(sym),
                    "funding": self.getFundingInfo(sym),
                    "trades_1s": self.getTrades(sym, lookback_seconds=lookback_seconds),
                    "l2": self.getL2(sym),
                }
                if include_user_state:
                    row["account"] = self._build_account_state_unlocked(
                        symbol=sym,
                        now_ms=now_ms,
                        fallback_rest=fallback_rest_for_user_state,
                    )
                    row["order_updates_1s"] = [
                        asdict(x)
                        for x in self.recent_order_updates.get(sym, [])
                        if x.event_time_ms >= (now_ms - max(1, int(lookback_seconds)) * 1000)
                    ]
                    row["user_stream"] = dict(user_status or {})
                out[sym] = row
            return out

    def _build_combined_streams(self) -> List[str]:
        streams: List[str] = []
        for sym in self.symbols:
            s = sym.lower()
            streams += [
                f"{s}@kline_{KLINE_INTERVAL_1M}",
                f"{s}@bookTicker",
                f"{s}@markPrice@1s",
                f"{s}@aggTrade",
                f"{s}@depth{L2_LEVELS}@100ms",
            ]
        return streams

    def _on_ws_message(self, msg: Dict[str, Any]) -> None:
        data = msg.get("data", msg)
        etype = data.get("e")
        if not etype:
            return

        with self._lock:
            if etype == "kline":
                self._handle_kline(data)
            elif etype == "bookTicker":
                self._handle_bookticker(data)
            elif etype == "markPriceUpdate":
                self._handle_markprice(data)
            elif etype == "aggTrade":
                self._handle_aggtrade(data)
            elif etype == "depthUpdate":
                self._handle_depth(data)

    def _handle_kline(self, d: Dict[str, Any]) -> None:
        k = d["k"]
        sym = k["s"]
        ev = Kline1m(
            symbol=sym,
            event_time_ms=int(d["E"]),
            start_time_ms=int(k["t"]),
            close_time_ms=int(k["T"]),
            interval=str(k["i"]),
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            base_vol=float(k["v"]),
            quote_vol=float(k.get("q", 0.0)),
            num_trades=int(k.get("n", 0)),
            is_closed=bool(k["x"]),
        )
        self.latest_kline_1m[sym] = ev

        if ev.is_closed and ev.interval == "1m":
            bucket = self._kline_bucket.setdefault(sym, [])
            bucket.append(ev)
            if len(bucket) > DERIVED_BAR_MINS:
                bucket.pop(0)

            if len(bucket) == DERIVED_BAR_MINS:
                bar10 = {
                    "symbol": sym,
                    "start_time_ms": bucket[0].start_time_ms,
                    "close_time_ms": bucket[-1].close_time_ms,
                    "open": bucket[0].open,
                    "high": max(x.high for x in bucket),
                    "low": min(x.low for x in bucket),
                    "close": bucket[-1].close,
                    "base_vol": sum(x.base_vol for x in bucket),
                    "quote_vol": sum(x.quote_vol for x in bucket),
                    "num_trades": sum(x.num_trades for x in bucket),
                }
                self.derived_10m_bars.append(bar10)

    def _handle_bookticker(self, d: Dict[str, Any]) -> None:
        sym = d["s"]
        self.latest_bbo[sym] = BBO(
            symbol=sym,
            event_time_ms=int(d["E"]),
            bid_px=float(d["b"]),
            bid_qty=float(d["B"]),
            ask_px=float(d["a"]),
            ask_qty=float(d["A"]),
        )

    def _handle_markprice(self, d: Dict[str, Any]) -> None:
        sym = d["s"]
        self.latest_funding[sym] = FundingInfo(
            symbol=sym,
            event_time_ms=int(d["E"]),
            mark_px=float(d["p"]),
            index_px=float(d["i"]),
            funding_rate=float(d["r"]),
            next_funding_time_ms=int(d["T"]),
        )

    def _handle_aggtrade(self, d: Dict[str, Any]) -> None:
        sym = d["s"]
        ev = AggTrade(
            symbol=sym,
            event_time_ms=int(d["E"]),
            trade_time_ms=int(d["T"]),
            agg_id=int(d["a"]),
            price=float(d["p"]),
            qty=float(d["q"]),
            is_buyer_maker=bool(d["m"]),
        )
        buf = self.recent_agg_trades.setdefault(sym, [])
        buf.append(ev)
        if len(buf) > 5000:
            del buf[:AGG_TRADE_EVENT_BUFFER_TRIM]
        self._append_agg_trade_event_unlocked(sym, ev)

    def _handle_depth(self, d: Dict[str, Any]) -> None:
        sym = d["s"]
        bids = [(float(px), float(qty)) for px, qty in d.get("b", [])[:L2_LEVELS]]
        asks = [(float(px), float(qty)) for px, qty in d.get("a", [])[:L2_LEVELS]]
        self.latest_l2[sym] = L2Depth(symbol=sym, event_time_ms=int(d["E"]), bids=bids, asks=asks)

    def getBars(self, symbol: str) -> Dict[str, Any]:
        k1 = self.latest_kline_1m.get(symbol)
        return asdict(k1) if k1 else None

    def getBBO(self, symbol: str) -> Optional[Dict[str, Any]]:
        b = self.latest_bbo.get(symbol)
        return asdict(b) if b else None

    def getFundingInfo(self, symbol: str) -> Optional[Dict[str, Any]]:
        f = self.latest_funding.get(symbol)
        return asdict(f) if f else None

    def getTrades(self, symbol: str, lookback_seconds: int = 1) -> List[Dict[str, Any]]:
        cutoff = _now_ms() - lookback_seconds * 1000
        buf = self.recent_agg_trades.get(symbol, [])
        return [asdict(t) for t in buf if t.trade_time_ms >= cutoff]

    def getL2(self, symbol: str) -> Optional[Dict[str, Any]]:
        l2 = self.latest_l2.get(symbol)
        return asdict(l2) if l2 else None
