# client.py
from __future__ import annotations

import contextlib
import threading
import time
from typing import Any, Dict, List, Optional

from aster.rest_api import Client as AsterRestClient
from aster.websocket.client.stream import WebsocketClient as AsterWebsocketClient
from twisted.internet import reactor

from .client_types import (
    DEFAULT_RUN_SECONDS,
    POLL_SECONDS,
    REST_RECONCILE_SECONDS,
    USER_STREAM_KEEPALIVE_SECONDS,
    USER_STREAM_RESTART_COOLDOWN_SECONDS,
    USER_STREAM_STALE_WARN_SECONDS,
    WS_STREAM_URL,
    AggTrade,
    BBO,
    FundingInfo,
    Kline1m,
    L2Depth,
    WsOrderUpdate,
    WsPosition,
    _now_ms,
)
from support.logs import CsvLogManager
from .market_data import MarketDataMixin
from .order_tracking import OrderTrackingMixin


class AsterClient(MarketDataMixin, OrderTrackingMixin):
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
        self.private_events_by_symbol: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
        self.recent_order_updates: Dict[str, List[WsOrderUpdate]] = {s: [] for s in symbols}
        self.agg_trade_events: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
        self._private_event_seq = 0
        self._agg_trade_event_seq = 0
        self.user_stream_listen_key: Optional[str] = None
        self.user_stream_started_ms = 0
        self.user_stream_last_event_ms = 0
        self.user_stream_last_account_event_ms = 0
        self.user_stream_last_order_event_ms = 0
        self.user_stream_last_keepalive_ms = 0
        self.user_stream_next_keepalive_ms = 0
        self.user_stream_last_restart_attempt_ms = 0
        self.user_stream_last_error: Optional[str] = None
        self.user_stream_expired = False
        self.user_stream_enabled = False
        self.user_stream_subscribed = False
        self._ws_account_seeded_from_rest = False
        self._last_user_stale_warn_ms = 0

        self._rest_positions_by_symbol: Dict[str, float] = {s: 0.0 for s in symbols}
        self._rest_total_margin_balance: Optional[float] = None
        self._rest_last_reconcile_ms = 0

        self._kline_bucket: Dict[str, List[Kline1m]] = {s: [] for s in symbols}
        self.derived_10m_bars: List[Dict[str, Any]] = []

        self.n_poll_snapshots = 0

    def _send_close_all(self, code: int = 1000, reason: str = "client shutdown") -> None:
        def _do() -> None:
            for factory in getattr(self.ws, "factories", {}).values():
                proto = getattr(factory, "protocol_instance", None)
                if proto is not None:
                    with contextlib.suppress(Exception):
                        proto.sendClose(code=code, reason=reason)

        reactor.callFromThread(_do)

    def _prepare_ws_for_shutdown(self) -> None:
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
