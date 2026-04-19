from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


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
PRIVATE_EVENT_BUFFER_SIZE = 4000
PRIVATE_EVENT_BUFFER_TRIM = 2000
AGG_TRADE_EVENT_BUFFER_SIZE = 8000
AGG_TRADE_EVENT_BUFFER_TRIM = 4000


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
