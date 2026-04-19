from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _to_bool(s: str) -> bool:
    return str(s).strip().lower() == "true"


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _read_secret(value_or_path: str | None) -> str:
    if not value_or_path:
        raise ValueError("Missing secret value/path in environment.")
    p = Path(value_or_path).expanduser()
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return str(value_or_path).strip()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_utc_ms(ts_ms: Optional[int]) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def _parse_hhmm_utc(s: str) -> int:
    parts = str(s).strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Expected HH:MM, got: {s!r}")
    hh = int(parts[0])
    mm = int(parts[1])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError(f"Invalid HH:MM, got: {s!r}")
    return hh * 60 + mm


def _load_tick_size_by_symbol(rest_client: Any, symbols: list[str]) -> Dict[str, float]:
    want = {s.strip().upper() for s in symbols if s.strip()}
    if not want:
        return {}
    try:
        info = rest_client.exchange_info()
        data = info.get("data") if isinstance(info, dict) and isinstance(info.get("data"), dict) else info
        syms = data.get("symbols") if isinstance(data, dict) else None
        if not isinstance(syms, list):
            print(f"[WARN] Unexpected exchange_info format for tick sizes: {info}")
            return {}

        out: Dict[str, float] = {}
        for s in syms:
            if not isinstance(s, dict):
                continue
            sym = str(s.get("symbol", "")).upper()
            if sym not in want:
                continue
            filters = s.get("filters")
            if not isinstance(filters, list):
                continue
            for f in filters:
                if not isinstance(f, dict):
                    continue
                if str(f.get("filterType", "")) != "PRICE_FILTER":
                    continue
                tick = _safe_float(f.get("tickSize"))
                if tick is not None and tick > 0:
                    out[sym] = float(tick)
                break
        return out
    except Exception as e:
        print(f"[WARN] failed to load tick sizes from exchangeInfo: {e}")
        return {}


def _utc_minute_of_day(ts_ms: int) -> tuple[int, datetime]:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.hour * 60 + dt.minute, dt


def _extract_update_time_ms(raw_query: Any) -> Optional[int]:
    if not isinstance(raw_query, dict):
        return None
    d = raw_query.get("data") if isinstance(raw_query.get("data"), dict) else raw_query
    t = _safe_float(d.get("updateTime")) if isinstance(d, dict) else None
    return int(t) if t is not None else None


def _resolve_email_smtp_pass() -> str:
    secret_dir = Path(os.getenv("ASTER_SECRET_DIR", "/opt/aster/.secrets"))
    p = secret_dir / "email_smtp_pass"
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return ""
