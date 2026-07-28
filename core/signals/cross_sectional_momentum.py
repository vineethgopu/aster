from __future__ import annotations

import json
import math
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, Optional


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _mean(xs: Deque[float]) -> float:
    return float(sum(xs) / len(xs)) if xs else 0.0


def _std(xs: Deque[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    var = sum((x - m) ** 2 for x in xs) / len(xs)
    return math.sqrt(max(0.0, var))


def _scaled_signal(z: float) -> float:
    # Rohrbach/Baz-style bounded trend score; extrema are scaled to +/-1.
    denom = math.sqrt(2.0) * math.exp(-0.5)
    return float(z * math.exp(-(z**2) / 4.0) / denom)


@dataclass(frozen=True)
class CrossSectionalMomentumConfig:
    symbols: list[str]
    bar_minutes: int = 240
    short_ema_windows: tuple[int, ...] = (8, 16, 32)
    long_ema_windows: tuple[int, ...] = (24, 48, 96)
    short_vol_window: int = 6
    long_signal_vol_window: int = 42
    leg_count: int = 3
    min_signal_abs: float = 0.0
    min_rebalance_weight_delta: float = 0.05
    target_gross_exposure: float = 1.0
    max_symbol_weight: float = 0.20
    weighting: str = "equal"
    vol_window_bars: int = 42
    max_spread_bps: float = 3.0
    max_funding_abs_bps: float = 2.0
    min_delta_notional_usd: float = 10.0
    enable_trading: bool = False
    notes: str = ""

    def __post_init__(self) -> None:
        symbols = [str(s).strip().upper() for s in self.symbols if str(s).strip()]
        if len(symbols) != len(set(symbols)):
            raise ValueError(f"symbols must be unique: {symbols}")
        if len(symbols) < 2 * self.leg_count:
            raise ValueError(
                f"Need at least {2 * self.leg_count} symbols for leg_count={self.leg_count}; "
                f"got {len(symbols)}"
            )
        if self.bar_minutes <= 0:
            raise ValueError("bar_minutes must be > 0")
        if len(self.short_ema_windows) != len(self.long_ema_windows):
            raise ValueError("short_ema_windows and long_ema_windows must have equal length")
        if any(x <= 0 for x in self.short_ema_windows + self.long_ema_windows):
            raise ValueError("EMA windows must be positive")
        if self.short_vol_window <= 0 or self.long_signal_vol_window <= 0:
            raise ValueError("normalization windows must be positive")
        if self.target_gross_exposure <= 0:
            raise ValueError("target_gross_exposure must be > 0")
        if self.max_symbol_weight <= 0:
            raise ValueError("max_symbol_weight must be > 0")
        if self.weighting not in {"equal", "inverse_vol"}:
            raise ValueError("weighting must be 'equal' or 'inverse_vol'")
        object.__setattr__(self, "symbols", symbols)


@dataclass
class _SymbolState:
    first_close: Optional[float] = None
    last_minute_close_ms: int = 0
    last_bar_bucket: Optional[int] = None
    pending_open: Optional[float] = None
    pending_high: Optional[float] = None
    pending_low: Optional[float] = None
    pending_close: Optional[float] = None
    indexed_history: Deque[float] = field(default_factory=deque)
    return_history: Deque[float] = field(default_factory=deque)
    short_emas: Dict[int, Optional[float]] = field(default_factory=dict)
    long_emas: Dict[int, Optional[float]] = field(default_factory=dict)
    y_history_by_pair: Dict[tuple[int, int], Deque[float]] = field(default_factory=dict)
    last_signal: Optional[float] = None


class CrossSectionalMomentumStrategy:
    """
    Portfolio-level EMA cross-sectional momentum signal.

    This object aggregates closed 1m klines into slower bars, computes the
    paper-style normalized EMA signal per symbol, then ranks symbols into a
    long/short target-weight basket. It intentionally emits target weights,
    not TP/SL/trailing bracket orders.
    """

    def __init__(self, cfg: CrossSectionalMomentumConfig) -> None:
        self.cfg = cfg
        max_price_window = max(cfg.short_vol_window, cfg.vol_window_bars)
        self._states: Dict[str, _SymbolState] = {}
        for sym in cfg.symbols:
            st = _SymbolState(
                indexed_history=deque(maxlen=max_price_window),
                return_history=deque(maxlen=cfg.vol_window_bars),
                short_emas={w: None for w in cfg.short_ema_windows},
                long_emas={w: None for w in cfg.long_ema_windows},
                y_history_by_pair={
                    (s, l): deque(maxlen=cfg.long_signal_vol_window)
                    for s, l in zip(cfg.short_ema_windows, cfg.long_ema_windows)
                },
            )
            self._states[sym] = st
        self._last_target_weights: Dict[str, float] = {sym: 0.0 for sym in cfg.symbols}
        self._last_rebalance_bucket: Optional[int] = None

    @classmethod
    def from_json_file(cls, path: str | Path) -> "CrossSectionalMomentumStrategy":
        raw = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError(f"Cross-sectional config must be a JSON object: {path}")
        cfg_raw = raw.get("cross_sectional_momentum", raw)
        if not isinstance(cfg_raw, dict):
            raise ValueError(f"cross_sectional_momentum must be a JSON object: {path}")
        cfg = CrossSectionalMomentumConfig(
            symbols=list(cfg_raw["symbols"]),
            bar_minutes=int(cfg_raw.get("bar_minutes", 240)),
            short_ema_windows=tuple(int(x) for x in cfg_raw.get("short_ema_windows", [8, 16, 32])),
            long_ema_windows=tuple(int(x) for x in cfg_raw.get("long_ema_windows", [24, 48, 96])),
            short_vol_window=int(cfg_raw.get("short_vol_window", 6)),
            long_signal_vol_window=int(cfg_raw.get("long_signal_vol_window", 42)),
            leg_count=int(cfg_raw.get("leg_count", 3)),
            min_signal_abs=float(cfg_raw.get("min_signal_abs", 0.0)),
            min_rebalance_weight_delta=float(cfg_raw.get("min_rebalance_weight_delta", 0.05)),
            target_gross_exposure=float(cfg_raw.get("target_gross_exposure", 1.0)),
            max_symbol_weight=float(cfg_raw.get("max_symbol_weight", 0.20)),
            weighting=str(cfg_raw.get("weighting", "equal")),
            vol_window_bars=int(cfg_raw.get("vol_window_bars", 42)),
            max_spread_bps=float(cfg_raw.get("max_spread_bps", 3.0)),
            max_funding_abs_bps=float(cfg_raw.get("max_funding_abs_bps", 2.0)),
            min_delta_notional_usd=float(cfg_raw.get("min_delta_notional_usd", 10.0)),
            enable_trading=bool(cfg_raw.get("enable_trading", False)),
            notes=str(cfg_raw.get("notes", "")),
        )
        return cls(cfg)

    @property
    def last_target_weights(self) -> Dict[str, float]:
        return dict(self._last_target_weights)

    def on_second(
        self,
        symbol_rows: Dict[str, Dict[str, Any]],
        now_ms: int,
    ) -> Optional[Dict[str, Any]]:
        closed_buckets: set[int] = set()
        for symbol in self.cfg.symbols:
            snap = symbol_rows.get(symbol) or {}
            bars = snap.get("bars") or {}
            bucket = self._update_symbol_from_1m_bar(symbol=symbol, bars_1m=bars)
            if bucket is not None:
                closed_buckets.add(bucket)

        if not closed_buckets:
            return None

        latest_bucket = max(closed_buckets)
        if self._last_rebalance_bucket is not None and latest_bucket <= self._last_rebalance_bucket:
            return None
        if not all(st.last_signal is not None for st in self._states.values()):
            return {
                "ts_ms": now_ms,
                "info": "warming_up",
                "bar_bucket": latest_bucket,
                "signals_ready": sum(st.last_signal is not None for st in self._states.values()),
                "signals_required": len(self._states),
            }

        target_weights, diagnostics = self._build_target_weights(symbol_rows=symbol_rows)
        if self._weight_delta(target_weights, self._last_target_weights) < self.cfg.min_rebalance_weight_delta:
            return {
                "ts_ms": now_ms,
                "info": "rebalance_skipped_small_delta",
                "bar_bucket": latest_bucket,
                "target_weights": target_weights,
                "previous_weights": self.last_target_weights,
                "diagnostics": diagnostics,
            }

        self._last_rebalance_bucket = latest_bucket
        previous = self._last_target_weights
        self._last_target_weights = target_weights
        return {
            "ts_ms": now_ms,
            "enter": True,
            "strategy": "cross_sectional_momentum",
            "bar_bucket": latest_bucket,
            "target_weights": target_weights,
            "previous_weights": dict(previous),
            "diagnostics": diagnostics,
        }

    def _update_symbol_from_1m_bar(self, symbol: str, bars_1m: Dict[str, Any]) -> Optional[int]:
        if not bars_1m or not bool(bars_1m.get("is_closed")):
            return None

        close_time_ms = int(bars_1m.get("close_time_ms") or 0)
        st = self._states[symbol]
        if close_time_ms <= st.last_minute_close_ms:
            return None
        st.last_minute_close_ms = close_time_ms

        close = _safe_float(bars_1m.get("close"))
        high = _safe_float(bars_1m.get("high")) or close
        low = _safe_float(bars_1m.get("low")) or close
        open_px = _safe_float(bars_1m.get("open")) or close
        if close is None or close <= 0:
            return None

        bucket_ms = self.cfg.bar_minutes * 60_000
        bucket = close_time_ms // bucket_ms
        if st.last_bar_bucket is None:
            st.last_bar_bucket = bucket
            st.pending_open = open_px
            st.pending_high = high
            st.pending_low = low
            st.pending_close = close
            return None

        if bucket == st.last_bar_bucket:
            st.pending_high = max(st.pending_high or high, high)
            st.pending_low = min(st.pending_low or low, low)
            st.pending_close = close
            return None

        completed_bucket = st.last_bar_bucket
        completed_close = st.pending_close
        st.last_bar_bucket = bucket
        st.pending_open = open_px
        st.pending_high = high
        st.pending_low = low
        st.pending_close = close

        if completed_close is not None and completed_close > 0:
            self._update_symbol_signal(symbol=symbol, close=float(completed_close))
            return int(completed_bucket)
        return None

    def _update_symbol_signal(self, symbol: str, close: float) -> None:
        st = self._states[symbol]
        if st.first_close is None:
            st.first_close = close
        indexed = close / float(st.first_close)
        prev_indexed = st.indexed_history[-1] if st.indexed_history else None
        st.indexed_history.append(indexed)
        if prev_indexed is not None and prev_indexed > 0:
            st.return_history.append(indexed / prev_indexed - 1.0)

        pair_signals: list[float] = []
        price_vol = _std(st.indexed_history) if len(st.indexed_history) >= self.cfg.short_vol_window else 0.0
        for short_n, long_n in zip(self.cfg.short_ema_windows, self.cfg.long_ema_windows):
            short_ema = self._update_ema(st.short_emas[short_n], indexed, short_n)
            long_ema = self._update_ema(st.long_emas[long_n], indexed, long_n)
            st.short_emas[short_n] = short_ema
            st.long_emas[long_n] = long_ema
            x = short_ema - long_ema

            if x == 0.0 or price_vol <= 0.0:
                y = 0.0
            else:
                y = x / price_vol
            y_hist = st.y_history_by_pair[(short_n, long_n)]
            y_hist.append(y)
            y_vol = _std(y_hist) if len(y_hist) >= self.cfg.long_signal_vol_window else 0.0
            z = 0.0 if x == 0.0 or price_vol <= 0.0 or y_vol <= 0.0 else y / y_vol
            pair_signals.append(_scaled_signal(z))

        if all(len(h) >= self.cfg.long_signal_vol_window for h in st.y_history_by_pair.values()):
            st.last_signal = sum(pair_signals) / len(pair_signals)

    @staticmethod
    def _update_ema(prev: Optional[float], value: float, window: int) -> float:
        alpha = 1.0 / float(window)
        return value if prev is None else alpha * value + (1.0 - alpha) * prev

    def _build_target_weights(
        self,
        symbol_rows: Dict[str, Dict[str, Any]],
    ) -> tuple[Dict[str, float], Dict[str, Any]]:
        candidates: dict[str, float] = {}
        blocked: dict[str, dict[str, Any]] = {}
        for symbol, st in self._states.items():
            signal = st.last_signal
            if signal is None or abs(signal) < self.cfg.min_signal_abs:
                continue
            ok, detail = self._market_filters(symbol=symbol, snap=symbol_rows.get(symbol) or {})
            if ok:
                candidates[symbol] = float(signal)
            else:
                blocked[symbol] = detail

        ranked = sorted(candidates.items(), key=lambda kv: kv[1], reverse=True)
        weights = {sym: 0.0 for sym in self.cfg.symbols}
        if len(ranked) < 2 * self.cfg.leg_count:
            return weights, {
                "signals": {sym: st.last_signal for sym, st in self._states.items()},
                "blocked": blocked,
                "info": "not_enough_tradeable_symbols",
                "tradeable_symbols": len(ranked),
            }

        longs = ranked[: self.cfg.leg_count]
        shorts = ranked[-self.cfg.leg_count :]
        long_weights = self._leg_weights(longs)
        short_weights = self._leg_weights(shorts)
        for sym, weight in long_weights.items():
            weights[sym] = weight
        for sym, weight in short_weights.items():
            weights[sym] = -weight
        return weights, {
            "signals": {sym: st.last_signal for sym, st in self._states.items()},
            "blocked": blocked,
            "longs": [sym for sym, _ in longs],
            "shorts": [sym for sym, _ in shorts],
            "gross_exposure": sum(abs(w) for w in weights.values()),
            "net_exposure": sum(weights.values()),
        }

    def _leg_weights(self, ranked_leg: list[tuple[str, float]]) -> Dict[str, float]:
        leg_gross = self.cfg.target_gross_exposure / 2.0
        if self.cfg.weighting == "inverse_vol":
            inv_vol = {}
            for sym, _ in ranked_leg:
                vol = _std(self._states[sym].return_history)
                inv_vol[sym] = 0.0 if vol <= 0.0 else 1.0 / vol
            total = sum(inv_vol.values())
            if total > 0:
                raw = {sym: leg_gross * v / total for sym, v in inv_vol.items()}
            else:
                raw = {sym: leg_gross / len(ranked_leg) for sym, _ in ranked_leg}
        else:
            raw = {sym: leg_gross / len(ranked_leg) for sym, _ in ranked_leg}

        capped = {sym: min(float(w), self.cfg.max_symbol_weight) for sym, w in raw.items()}
        total_capped = sum(capped.values())
        if total_capped <= 0:
            return capped
        scale = min(1.0, leg_gross / total_capped)
        return {sym: w * scale for sym, w in capped.items()}

    def _market_filters(self, symbol: str, snap: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        bbo = snap.get("bbo") or {}
        funding = snap.get("funding") or {}
        bid = _safe_float(bbo.get("bid_px"))
        ask = _safe_float(bbo.get("ask_px"))
        mid = _safe_float(bbo.get("mid"))
        if mid is None and bid is not None and ask is not None:
            mid = 0.5 * (bid + ask)
        if bid is None or ask is None or mid is None or mid <= 0:
            return False, {"reason": "missing_bbo"}
        spread_bps = 1e4 * max(0.0, ask - bid) / mid
        funding_rate = _safe_float(funding.get("funding_rate"))
        funding_bps = None if funding_rate is None else funding_rate * 1e4
        ok = spread_bps <= self.cfg.max_spread_bps and (
            funding_bps is not None and abs(funding_bps) <= self.cfg.max_funding_abs_bps
        )
        return ok, {
            "spread_bps": spread_bps,
            "spread_ok": spread_bps <= self.cfg.max_spread_bps,
            "funding_bps": funding_bps,
            "funding_ok": funding_bps is not None and abs(funding_bps) <= self.cfg.max_funding_abs_bps,
        }

    @staticmethod
    def _weight_delta(a: Dict[str, float], b: Dict[str, float]) -> float:
        keys = set(a) | set(b)
        return sum(abs(float(a.get(k, 0.0)) - float(b.get(k, 0.0))) for k in keys)
