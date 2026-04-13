# client.py
from __future__ import annotations

import contextlib
import logging
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

# aster connector REST + WS
from aster.rest_api import Client as AsterRestClient
from aster.websocket.client.stream import WebsocketClient as AsterWebsocketClient

# graceful close uses reactor
from twisted.internet import reactor

from logs import CsvLogManager


WS_STREAM_URL = "wss://fstream.asterdex.com"
DEFAULT_RUN_SECONDS = 10 * 60
POLL_SECONDS = 1.0

KLINE_INTERVAL_1M = "1m"
DERIVED_BAR_MINS = 10
L2_LEVELS = 5
USER_STREAM_KEEPALIVE_SECONDS = 30 * 60
USER_STREAM_RESTART_COOLDOWN_SECONDS = 5
USER_STREAM_STALE_WARN_SECONDS = 90
REST_RECONCILE_SECONDS = 10


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_bool_or_none(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return bool(v)


@dataclass
class BBO:
    symbol: str
    event_time_ms: int
    bid_px: float
    bid_qty: float
    ask_px: float
    ask_qty: float


@dataclass
class FundingInfo:
    symbol: str
    event_time_ms: int
    mark_px: float
    index_px: float
    funding_rate: float
    next_funding_time_ms: int


@dataclass
class AggTrade:
    symbol: str
    event_time_ms: int
    trade_time_ms: int
    agg_id: int
    price: float
    qty: float
    is_buyer_maker: bool


@dataclass
class Kline1m:
    symbol: str
    event_time_ms: int
    start_time_ms: int
    close_time_ms: int
    interval: str
    open: float
    high: float
    low: float
    close: float
    base_vol: float
    quote_vol: float
    num_trades: int
    is_closed: bool


@dataclass
class L2Depth:
    symbol: str
    event_time_ms: int
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]


@dataclass
class WsPosition:
    symbol: str
    event_time_ms: int
    position_amt: float
    entry_price: Optional[float]
    unrealized_pnl: Optional[float]
    margin_type: Optional[str]
    isolated_wallet: Optional[float]


@dataclass
class WsOrderUpdate:
    symbol: str
    order_id: int
    client_order_id: Optional[str]
    event_time_ms: int
    transaction_time_ms: Optional[int]
    side: Optional[str]
    order_type: Optional[str]
    time_in_force: Optional[str]
    status: Optional[str]
    execution_type: Optional[str]
    orig_qty: Optional[float]
    cum_filled_qty: Optional[float]
    last_filled_qty: Optional[float]
    avg_price: Optional[float]
    last_price: Optional[float]
    reduce_only: Optional[bool]
    close_position: Optional[bool]
    raw: Dict[str, Any]


class AsterClient:
    """
    REST snapshot + WS streaming cache + 1s polling rows to CSVs.
    """

    def __init__(
        self,
        symbols: List[str],
        log_dir: str = "./logs",
        delete_logs: bool = False,
        rest_key: Optional[str] = None,
        rest_secret: Optional[str] = None,
        stream_url: str = WS_STREAM_URL,
        poll_seconds: float = POLL_SECONDS,
        user_stream_keepalive_seconds: int = USER_STREAM_KEEPALIVE_SECONDS,
        user_stream_restart_cooldown_seconds: int = USER_STREAM_RESTART_COOLDOWN_SECONDS,
        user_stream_stale_warn_seconds: int = USER_STREAM_STALE_WARN_SECONDS,
        rest_reconcile_seconds: int = REST_RECONCILE_SECONDS,
    ) -> None:
        self.symbols = symbols
        self.poll_seconds = poll_seconds
        self.user_stream_keepalive_seconds = max(60, int(user_stream_keepalive_seconds))
        self.user_stream_restart_cooldown_seconds = max(1, int(user_stream_restart_cooldown_seconds))
        self.user_stream_stale_warn_seconds = max(10, int(user_stream_stale_warn_seconds))
        self.rest_reconcile_seconds = max(1, int(rest_reconcile_seconds))

        self.rest = AsterRestClient(key=rest_key, secret=rest_secret)
        self.ws = AsterWebsocketClient(stream_url=stream_url)
        self.logger = CsvLogManager(log_dir=log_dir, delete_logs=delete_logs)

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._intentional_shutdown = False
        self._has_auth = bool(rest_key)
        self._rest_key = rest_key
        self._rest_secret = rest_secret

        # latest caches
        self.latest_bbo: Dict[str, BBO] = {}
        self.latest_funding: Dict[str, FundingInfo] = {}
        self.latest_kline_1m: Dict[str, Kline1m] = {}
        self.latest_l2: Dict[str, L2Depth] = {}
        self.recent_agg_trades: Dict[str, List[AggTrade]] = {s: [] for s in symbols}
        self.user_balances: Dict[str, Dict[str, Any]] = {}
        self.user_positions: Dict[str, WsPosition] = {}
        self.user_orders_by_id: Dict[int, WsOrderUpdate] = {}
        self.user_orders_by_symbol: Dict[str, Dict[int, WsOrderUpdate]] = {s: {} for s in symbols}
        self.recent_user_events: List[Dict[str, Any]] = []
        self.recent_order_updates: Dict[str, List[WsOrderUpdate]] = {s: [] for s in symbols}
        self.user_stream_listen_key: Optional[str] = None
        self.user_stream_started_ms: int = 0
        self.user_stream_last_event_ms: int = 0
        self.user_stream_last_account_event_ms: int = 0
        self.user_stream_last_order_event_ms: int = 0
        self.user_stream_last_keepalive_ms: int = 0
        self.user_stream_next_keepalive_ms: int = 0
        self.user_stream_last_restart_attempt_ms: int = 0
        self.user_stream_last_error: Optional[str] = None
        self.user_stream_expired: bool = False
        self.user_stream_enabled: bool = False
        self.user_stream_subscribed: bool = False
        self._ws_account_seeded_from_rest: bool = False
        self._last_user_stale_warn_ms: int = 0

        # REST fallback state
        self._rest_positions_by_symbol: Dict[str, float] = {s: 0.0 for s in symbols}
        self._rest_total_margin_balance: Optional[float] = None
        self._rest_last_reconcile_ms: int = 0

        self._kline_bucket: Dict[str, List[Kline1m]] = {s: [] for s in symbols}
        self.derived_10m_bars: List[Dict[str, Any]] = []

        self.n_poll_snapshots: int = 0

    # -------------------------
    # REST snapshot
    # -------------------------
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
                    if isinstance(k_last, list) and len(k_last) >= 9:
                        start_ms = _to_int(k_last[0])
                        open_px = _to_float(k_last[1])
                        high_px = _to_float(k_last[2])
                        low_px = _to_float(k_last[3])
                        close_px = _to_float(k_last[4])
                        base_vol = _to_float(k_last[5])
                        close_ms = _to_int(k_last[6])
                        quote_vol = _to_float(k_last[7])
                        n_trades = _to_int(k_last[8])
                        if None not in (start_ms, open_px, high_px, low_px, close_px, base_vol, close_ms, quote_vol, n_trades):
                            self.latest_kline_1m[sym] = Kline1m(
                                symbol=sym,
                                event_time_ms=ts_ms,
                                start_time_ms=start_ms,
                                close_time_ms=close_ms,
                                interval=KLINE_INTERVAL_1M,
                                open=open_px,
                                high=high_px,
                                low=low_px,
                                close=close_px,
                                base_vol=base_vol,
                                quote_vol=quote_vol,
                                num_trades=n_trades,
                                is_closed=(_now_ms() >= close_ms),
                            )
                    elif isinstance(k_last, dict):
                        start_ms = _to_int(k_last.get("openTime", k_last.get("t")))
                        close_ms = _to_int(k_last.get("closeTime", k_last.get("T")))
                        open_px = _to_float(k_last.get("open", k_last.get("o")))
                        high_px = _to_float(k_last.get("high", k_last.get("h")))
                        low_px = _to_float(k_last.get("low", k_last.get("l")))
                        close_px = _to_float(k_last.get("close", k_last.get("c")))
                        base_vol = _to_float(k_last.get("volume", k_last.get("v")))
                        quote_vol = _to_float(k_last.get("quoteAssetVolume", k_last.get("q", 0.0)))
                        n_trades = _to_int(k_last.get("numTrades", k_last.get("n", 0)))
                        if None not in (start_ms, close_ms, open_px, high_px, low_px, close_px, base_vol, quote_vol, n_trades):
                            self.latest_kline_1m[sym] = Kline1m(
                                symbol=sym,
                                event_time_ms=ts_ms,
                                start_time_ms=start_ms,
                                close_time_ms=close_ms,
                                interval=KLINE_INTERVAL_1M,
                                open=open_px,
                                high=high_px,
                                low=low_px,
                                close=close_px,
                                base_vol=base_vol,
                                quote_vol=quote_vol,
                                num_trades=n_trades,
                                is_closed=(_now_ms() >= close_ms),
                            )

                trades = payload.get("aggTrades")
                if isinstance(trades, list):
                    parsed_trades: List[AggTrade] = []
                    for t in trades:
                        if not isinstance(t, dict):
                            continue
                        trade_ms = _to_int(t.get("T", t.get("time")))
                        price = _to_float(t.get("p", t.get("price")))
                        qty = _to_float(t.get("q", t.get("qty")))
                        agg_id = _to_int(t.get("a", t.get("aggId", t.get("id"))))
                        maker_flag = t.get("m", t.get("isBuyerMaker"))
                        if None in (trade_ms, price, qty, agg_id) or maker_flag is None:
                            continue
                        parsed_trades.append(
                            AggTrade(
                                symbol=sym,
                                event_time_ms=trade_ms,
                                trade_time_ms=trade_ms,
                                agg_id=agg_id,
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

    # -------------------------
    # User stream state
    # -------------------------
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
            # Keepalive should succeed regularly; otherwise stream health is unknown.
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

        positions_out: Dict[str, Tuple[float, Optional[float], Optional[float], Optional[str], Optional[float], int]] = {}
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

        # Seed from REST so position truth is available before first user event arrives.
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

        # Renew listen key periodically.
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

        # If user stream is unhealthy, refresh REST account truth and attempt restart.
        unhealthy = bool(expired or last_error or (enabled and not subscribed))
        if unhealthy:
            self.reconcile_account_state_from_rest(force=True, merge_into_ws=True)
            cooldown_ms = self.user_stream_restart_cooldown_seconds * 1000
            if now_ms - last_restart_ms >= cooldown_ms:
                logging.warning("[USER_STREAM] unhealthy, attempting restart.")
                self.start_user_stream()

        # Visibility guardrail: if stream is quiet for too long, refresh REST caches.
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
            else:
                # Keep unknown event types for observability.
                pass

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
            # Ignore out-of-order older updates.
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

    def get_order_update(self, order_id: Optional[int]) -> Optional[Dict[str, Any]]:
        if order_id is None:
            return None
        with self._lock:
            upd = self.user_orders_by_id.get(int(order_id))
            return asdict(upd) if upd is not None else None

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

        # Prefer earliest fill event time in case multiple updates are present.
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

    def get_symbol_rows(
        self,
        *,
        lookback_seconds: int = 1,
        include_user_state: bool = False,
        fallback_rest_for_user_state: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        now_ms = _now_ms()
        if include_user_state and fallback_rest_for_user_state and self._has_auth:
            # Keep fallback caches fresh for unhealthy stream states.
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

    # -------------------------
    # WS subscribe helpers
    # -------------------------
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

    # -------------------------
    # WS handlers
    # -------------------------
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
            del buf[:2500]

    def _handle_depth(self, d: Dict[str, Any]) -> None:
        sym = d["s"]
        bids = [(float(px), float(qty)) for px, qty in d.get("b", [])[:L2_LEVELS]]
        asks = [(float(px), float(qty)) for px, qty in d.get("a", [])[:L2_LEVELS]]
        self.latest_l2[sym] = L2Depth(symbol=sym, event_time_ms=int(d["E"]), bids=bids, asks=asks)

    # -------------------------
    # Public getters (used by logger)
    # -------------------------
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

    # -------------------------
    # Graceful close (avoid 1006 on shutdown)
    # -------------------------
    def _send_close_all(self, code: int = 1000, reason: str = "client shutdown") -> None:
        def _do():
            for factory in getattr(self.ws, "factories", {}).values():
                proto = getattr(factory, "protocol_instance", None)
                if proto is not None:
                    with contextlib.suppress(Exception):
                        proto.sendClose(code=code, reason=reason)
        reactor.callFromThread(_do)

    def _prepare_ws_for_shutdown(self) -> None:
        # The upstream connector retries/logs on any disconnect. During our own
        # teardown, disable reconnect callbacks to avoid false error noise.
        for factory in getattr(self.ws, "factories", {}).values():
            with contextlib.suppress(Exception):
                factory.continueTrying = False
            with contextlib.suppress(Exception):
                factory.stopTrying()
            with contextlib.suppress(Exception):
                factory.clientConnectionLost = lambda connector, reason: None
            with contextlib.suppress(Exception):
                factory.clientConnectionFailed = lambda connector, reason: None

            proto = getattr(factory, "protocol_instance", None)
            if proto is not None:
                with contextlib.suppress(Exception):
                    proto.onClose = lambda wasClean, code, reason: None

    def graceful_shutdown(self, handshake_wait_seconds: float = 0.8) -> None:
        self._intentional_shutdown = True
        self.close_user_stream()
        self._prepare_ws_for_shutdown()
        try:
            self._send_close_all(code=1000, reason="graceful client shutdown")
            time.sleep(handshake_wait_seconds)
        finally:
            with contextlib.suppress(Exception):
                self.ws.stop()

    # -------------------------
    # Main run
    # -------------------------
    def run(self, run_seconds: int = DEFAULT_RUN_SECONDS) -> Dict[str, Any]:
        startup = self.rest_snapshot()
        self._seed_from_rest_snapshot(startup)

        self.ws.start()
        streams = self._build_combined_streams()
        self.ws.live_subscribe(streams, id=1, callback=self._on_ws_message)
        if self._has_auth:
            self.start_user_stream()

        start = time.time()
        try:
            while (time.time() - start) < run_seconds and not self._stop_event.is_set():
                ts = _now_ms()
                if self._has_auth:
                    self.poll_user_stream_maintenance(now_ms=ts)

                symbol_rows = self.get_symbol_rows(
                    lookback_seconds=1,
                    include_user_state=self._has_auth,
                    fallback_rest_for_user_state=self._has_auth,
                )

                # write 5 CSV rows per symbol per second
                self.logger.write_second(ts, symbol_rows)
                self.n_poll_snapshots += 1
                if self.n_poll_snapshots % 60 == 0:
                    print(self.n_poll_snapshots)

                time.sleep(self.poll_seconds)

        finally:
            self._stop_event.set()
            self.logger.close()
            self.graceful_shutdown(handshake_wait_seconds=0.8)

        return {
            "startup_snapshot": startup,
            "n_poll_snapshots": self.n_poll_snapshots,
            "n_derived_10m": len(self.derived_10m_bars),
        }
