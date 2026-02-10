# client.py
from __future__ import annotations

import contextlib
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


def _now_ms() -> int:
    return int(time.time() * 1000)


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


class AsterClient:
    """
    REST snapshot + WS streaming cache + 1s polling rows to CSVs.
    """

    def __init__(
        self,
        symbols: List[str],
        log_dir: str = "./logs",
        rest_key: Optional[str] = None,
        rest_secret: Optional[str] = None,
        stream_url: str = WS_STREAM_URL,
        poll_seconds: float = POLL_SECONDS,
    ) -> None:
        self.symbols = symbols
        self.poll_seconds = poll_seconds

        self.rest = AsterRestClient(key=rest_key, secret=rest_secret)
        self.ws = AsterWebsocketClient(stream_url=stream_url)
        self.logger = CsvLogManager(log_dir=log_dir)

        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        # latest caches
        self.latest_bbo: Dict[str, BBO] = {}
        self.latest_funding: Dict[str, FundingInfo] = {}
        self.latest_kline_1m: Dict[str, Kline1m] = {}
        self.latest_l2: Dict[str, L2Depth] = {}
        self.recent_agg_trades: Dict[str, List[AggTrade]] = {s: [] for s in symbols}

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

    def graceful_shutdown(self, handshake_wait_seconds: float = 0.8) -> None:
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

        self.ws.start()
        streams = self._build_combined_streams()
        self.ws.live_subscribe(streams, id=1, callback=self._on_ws_message)

        start = time.time()
        try:
            while (time.time() - start) < run_seconds and not self._stop_event.is_set():
                ts = _now_ms()

                with self._lock:
                    symbol_rows: Dict[str, Dict] = {}
                    for sym in self.symbols:
                        symbol_rows[sym] = {
                            "bars": self.getBars(sym),
                            "bbo": self.getBBO(sym),
                            "funding": self.getFundingInfo(sym),
                            "trades_1s": self.getTrades(sym, lookback_seconds=1),
                            "l2": self.getL2(sym),
                        }

                # write 5 CSV rows per symbol per second
                self.logger.write_second(ts, symbol_rows)
                self.n_poll_snapshots += 1
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
