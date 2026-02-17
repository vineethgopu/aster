from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _bps_ret(px: float, ref: float) -> float:
    if ref == 0:
        return 0.0
    return 1e4 * (px / ref - 1.0)


def _rs_var(o: float, h: float, l: float, c: float) -> float:
    # Rogers-Satchell per-bar variance component.
    if o <= 0 or h <= 0 or l <= 0 or c <= 0:
        return 0.0
    lo = math.log
    return (lo(h / o) * lo(h / c)) + (lo(l / o) * lo(l / c))


def _rs_vol_bps(rs_vars: Deque[float]) -> Optional[float]:
    if not rs_vars:
        return None
    # sqrt(1/T) * sqrt(sum(var_i)) == sqrt(mean(var_i))
    mean_var = sum(rs_vars) / len(rs_vars)
    if mean_var < 0:
        mean_var = 0.0
    return 1e4 * math.sqrt(mean_var)


@dataclass
class StrategyConfig:
    k: float
    t_window: int
    n: float
    v_window: int
    max_spread: float = 0.2
    max_funding_abs_bps: float = 1.5


class Strategy:
    def __init__(self, cfg: StrategyConfig, symbols: list[str]) -> None:
        self.cfg = cfg
        self.symbols = symbols

        self._rs_vars: Dict[str, Deque[float]] = {
            s: deque(maxlen=max(2, cfg.t_window)) for s in symbols
        }
        self._vols: Dict[str, Deque[float]] = {
            s: deque(maxlen=max(2, cfg.v_window)) for s in symbols
        }
        self._last_closed_close_ms: Dict[str, int] = {s: 0 for s in symbols}
        self._prev_close: Dict[str, Optional[float]] = {s: None for s in symbols}

    def on_second(
        self,
        symbol: str,
        bars_1m: Optional[Dict[str, Any]],
        bbo: Optional[Dict[str, Any]],
        funding: Optional[Dict[str, Any]],
        now_ms: int,
    ) -> Optional[Dict[str, Any]]:
        if not bars_1m:
            return None

        is_closed = bool(bars_1m.get("is_closed"))
        close_time_ms = int(bars_1m.get("close_time_ms") or 0)
        if not is_closed or close_time_ms <= self._last_closed_close_ms[symbol]:
            return None
        self._last_closed_close_ms[symbol] = close_time_ms

        o = _safe_float(bars_1m.get("open"))
        h = _safe_float(bars_1m.get("high"))
        l = _safe_float(bars_1m.get("low"))
        c = _safe_float(bars_1m.get("close"))
        v = _safe_float(bars_1m.get("base_vol"))
        if None in (o, h, l, c, v):
            return {"symbol": symbol, "ts_ms": now_ms, "error": "missing_ohlcv"}

        self._rs_vars[symbol].append(_rs_var(o, h, l, c))
        self._vols[symbol].append(v)

        prev_close = self._prev_close[symbol]
        self._prev_close[symbol] = c
        if prev_close is None or prev_close <= 0:
            return {"symbol": symbol, "ts_ms": now_ms, "info": "first_close_seen"}

        ret_bps = _bps_ret(c, prev_close)
        rs_vol_bps_partial = _rs_vol_bps(self._rs_vars[symbol])
        avg_vol_partial = (sum(self._vols[symbol]) / len(self._vols[symbol])) if self._vols[symbol] else None

        rs_ready = len(self._rs_vars[symbol]) >= self.cfg.t_window
        vol_ready = len(self._vols[symbol]) >= self.cfg.v_window
        if not (rs_ready and vol_ready):
            return {
                "symbol": symbol,
                "ts_ms": now_ms,
                "info": "warming_up",
                "ret_bps": ret_bps,
                "rs_vol_bps": rs_vol_bps_partial,
                "avg_base_vol": avg_vol_partial,
                "rs_bars": len(self._rs_vars[symbol]),
                "rs_required": self.cfg.t_window,
                "vol_bars": len(self._vols[symbol]),
                "vol_required": self.cfg.v_window,
            }

        rs_vol_bps = _rs_vol_bps(self._rs_vars[symbol])
        avg_vol = (sum(self._vols[symbol]) / len(self._vols[symbol])) if self._vols[symbol] else None

        side: Optional[str] = None
        indicator1 = False
        if rs_vol_bps is not None:
            thresh = self.cfg.k * rs_vol_bps
            if ret_bps > thresh:
                side = "BUY"
                indicator1 = True
            elif ret_bps < -thresh:
                side = "SELL"
                indicator1 = True

        indicator2 = bool(avg_vol is not None and avg_vol > 0 and v > self.cfg.n * avg_vol)
        blockers_ok, blockers = self._check_blockers(bbo=bbo, funding=funding, side=side)
        should_enter = bool(indicator1 and indicator2 and blockers_ok and side in ("BUY", "SELL"))

        return {
            "symbol": symbol,
            "ts_ms": now_ms,
            "close_time_ms": close_time_ms,
            "ret_bps": ret_bps,
            "rs_vol_bps": rs_vol_bps,
            "bar_base_vol": v,
            "avg_base_vol": avg_vol,
            "indicator1": indicator1,
            "indicator2": indicator2,
            "side": side,
            "blockers_ok": blockers_ok,
            "blockers": blockers,
            "enter": should_enter,
        }

    def _check_blockers(
        self,
        bbo: Optional[Dict[str, Any]],
        funding: Optional[Dict[str, Any]],
        side: Optional[str],
    ) -> Tuple[bool, Dict[str, Any]]:
        detail: Dict[str, Any] = {}

        bid = _safe_float((bbo or {}).get("bid_px"))
        ask = _safe_float((bbo or {}).get("ask_px"))
        mid = _safe_float((bbo or {}).get("mid"))
        if mid is None and bid is not None and ask is not None:
            mid = 0.5 * (bid + ask)

        if bid is None or ask is None or mid is None or mid <= 0:
            detail["spread_ok"] = False
            detail["spread"] = None
            detail["spread_bps"] = None
            spread_ok = False
        else:
            spread = ask - bid
            spread_bps = 1e4 * spread / mid
            spread_ok = spread <= self.cfg.max_spread
            detail["spread"] = spread
            detail["spread_bps"] = spread_bps
            detail["spread_ok"] = spread_ok

        fr = _safe_float((funding or {}).get("funding_rate"))
        if fr is None:
            funding_ok = False
            funding_bps = None
        else:
            funding_bps = fr * 1e4
            funding_ok = abs(funding_bps) <= self.cfg.max_funding_abs_bps
        detail["funding_bps"] = funding_bps
        detail["funding_ok"] = funding_ok

        mark = _safe_float((funding or {}).get("mark_px"))
        opening_loss_bps = None
        if side is None:
            opening_ok = True
        elif mark is None or bid is None or ask is None:
            opening_ok = False
        else:
            if side == "BUY":
                opening_loss_bps = _bps_ret(ask, mark)
            else:
                opening_loss_bps = _bps_ret(mark, bid)
            spread_bps = detail.get("spread_bps")
            if spread_bps is None:
                opening_ok = False
            else:
                max_opening_loss_bps = min(10.0, max(5.0, 2.0 * spread_bps))
                opening_ok = opening_loss_bps <= max_opening_loss_bps
        detail["opening_loss_bps"] = opening_loss_bps
        detail["opening_ok"] = opening_ok

        return bool(spread_ok and funding_ok and opening_ok), detail
