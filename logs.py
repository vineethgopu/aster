# log.py
from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional


def fmt_dt_utc(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


class CsvAppender:
    """
    Buffered CSV appender. Writes header once. Flushes every N rows.
    """
    def __init__(self, path: str, fieldnames: List[str], flush_every: int = 200) -> None:
        self.path = path
        self.fieldnames = fieldnames
        self.flush_every = flush_every
        self._buf: List[Dict] = []

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        needs_header = (not os.path.exists(path)) or (os.path.getsize(path) == 0)
        if needs_header:
            with open(self.path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=self.fieldnames)
                w.writeheader()

    def append(self, row: Dict) -> None:
        self._buf.append(row)
        if len(self._buf) >= self.flush_every:
            self.flush()

    def append_many(self, rows: Iterable[Dict]) -> None:
        for r in rows:
            self.append(r)

    def flush(self) -> None:
        if not self._buf:
            return
        with open(self.path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.fieldnames)
            w.writerows(self._buf)
        self._buf.clear()

    def close(self) -> None:
        self.flush()


@dataclass
class LogWriters:
    bars: CsvAppender
    bbo: CsvAppender
    funding: CsvAppender
    trades_1s: CsvAppender
    depth5: CsvAppender


class CsvLogManager:
    """
    Owns 5 CSV appenders (one per "etype category" / function).
    """
    def __init__(self, log_dir: str = "./logs") -> None:
        os.makedirs(log_dir, exist_ok=True)

        self.writers = LogWriters(
            bars=CsvAppender(
                os.path.join(log_dir, "kline.csv"),
                fieldnames=[
                    "ts_unix_ms", "ts_dt_utc", "symbol",
                    "k1_start_ms", "k1_close_ms", "k1_open", "k1_high", "k1_low", "k1_close",
                    "k1_base_vol", "k1_quote_vol", "k1_trades", "k1_closed",
                    "k10_start_ms", "k10_close_ms", "k10_open", "k10_high", "k10_low", "k10_close",
                    "k10_base_vol", "k10_quote_vol", "k10_trades",
                ],
            ),
            bbo=CsvAppender(
                os.path.join(log_dir, "bookTicker.csv"),
                fieldnames=[
                    "ts_unix_ms", "ts_dt_utc", "symbol",
                    "bid_px", "bid_qty", "ask_px", "ask_qty", "spread", "mid",
                ],
            ),
            funding=CsvAppender(
                os.path.join(log_dir, "markPrice.csv"),
                fieldnames=[
                    "ts_unix_ms", "ts_dt_utc", "symbol",
                    "mark_px", "index_px", "funding_rate", "next_funding_time_ms", "mark_index_bps",
                ],
            ),
            trades_1s=CsvAppender(
                os.path.join(log_dir, "aggTrade_1s.csv"),
                fieldnames=[
                    "ts_unix_ms", "ts_dt_utc", "symbol",
                    "n_trades_1s", "sum_qty_1s", "vwap_1s",
                    "buy_qty_1s", "sell_qty_1s",
                    "buy_notional_1s", "sell_notional_1s",
                ],
            ),
            depth5=CsvAppender(
                os.path.join(log_dir, "depth5.csv"),
                fieldnames=(
                    ["ts_unix_ms", "ts_dt_utc", "symbol"]
                    + [f"bid{i}_px" for i in range(1, 6)] + [f"bid{i}_qty" for i in range(1, 6)]
                    + [f"ask{i}_px" for i in range(1, 6)] + [f"ask{i}_qty" for i in range(1, 6)]
                    + ["obi5"]
                ),
            ),
        )

    def write_second(
        self,
        ts_ms: int,
        symbol_rows: Dict[str, Dict],
    ) -> None:
        """
        symbol_rows[symbol] = {
           "bars": {"kline_1m": {...}, "bar_10m": {...}},
           "bbo": {...},
           "funding": {...},
           "trades_1s": [ {aggTrade}, ... ],
           "l2": {"bids":[(px,qty)...], "asks":[...]} or None
        }
        """
        ts_dt = fmt_dt_utc(ts_ms)

        bars_rows = []
        bbo_rows = []
        funding_rows = []
        trades_rows = []
        l2_rows = []

        for sym, snap in symbol_rows.items():
            # ----- Bars -----
            bars = snap.get("bars") or {}
            k1 = (bars.get("kline_1m") or {})
            k10 = (bars.get("bar_10m") or {})

            bars_rows.append({
                "ts_unix_ms": ts_ms, "ts_dt_utc": ts_dt, "symbol": sym,
                "k1_start_ms": k1.get("start_time_ms"),
                "k1_close_ms": k1.get("close_time_ms"),
                "k1_open": k1.get("open"),
                "k1_high": k1.get("high"),
                "k1_low": k1.get("low"),
                "k1_close": k1.get("close"),
                "k1_base_vol": k1.get("base_vol"),
                "k1_quote_vol": k1.get("quote_vol"),
                "k1_trades": k1.get("num_trades"),
                "k1_closed": k1.get("is_closed"),
                "k10_start_ms": k10.get("start_time_ms"),
                "k10_close_ms": k10.get("close_time_ms"),
                "k10_open": k10.get("open"),
                "k10_high": k10.get("high"),
                "k10_low": k10.get("low"),
                "k10_close": k10.get("close"),
                "k10_base_vol": k10.get("base_vol"),
                "k10_quote_vol": k10.get("quote_vol"),
                "k10_trades": k10.get("num_trades"),
            })

            # ----- BBO -----
            bbo = snap.get("bbo") or {}
            bid_px = bbo.get("bid_px")
            ask_px = bbo.get("ask_px")
            spread = (ask_px - bid_px) if isinstance(bid_px, (int, float)) and isinstance(ask_px, (int, float)) else None
            mid = ((ask_px + bid_px) / 2.0) if spread is not None else None

            bbo_rows.append({
                "ts_unix_ms": ts_ms, "ts_dt_utc": ts_dt, "symbol": sym,
                "bid_px": bid_px, "bid_qty": bbo.get("bid_qty"),
                "ask_px": ask_px, "ask_qty": bbo.get("ask_qty"),
                "spread": spread, "mid": mid,
            })

            # ----- Funding / Mark -----
            f = snap.get("funding") or {}
            mark_px = f.get("mark_px")
            index_px = f.get("index_px")
            mark_index_bps = None
            if isinstance(mark_px, (int, float)) and isinstance(index_px, (int, float)) and index_px != 0:
                mark_index_bps = 1e4 * (mark_px / index_px - 1.0)

            funding_rows.append({
                "ts_unix_ms": ts_ms, "ts_dt_utc": ts_dt, "symbol": sym,
                "mark_px": mark_px,
                "index_px": index_px,
                "funding_rate": f.get("funding_rate"),
                "next_funding_time_ms": f.get("next_funding_time_ms"),
                "mark_index_bps": mark_index_bps,
            })

            # ----- Trades (aggregate over last 1s) -----
            trades = snap.get("trades_1s") or []
            n = len(trades)
            sum_qty = 0.0
            sum_notional = 0.0
            buy_qty = sell_qty = 0.0
            buy_notional = sell_notional = 0.0

            for t in trades:
                px = t.get("price")
                qty = t.get("qty")
                is_buyer_maker = t.get("is_buyer_maker")
                if not isinstance(px, (int, float)) or not isinstance(qty, (int, float)):
                    continue
                sum_qty += qty
                sum_notional += px * qty

                # Convention: buyer-mkr => taker was seller
                if is_buyer_maker:
                    sell_qty += qty
                    sell_notional += px * qty
                else:
                    buy_qty += qty
                    buy_notional += px * qty

            vwap = (sum_notional / sum_qty) if sum_qty > 0 else None

            trades_rows.append({
                "ts_unix_ms": ts_ms, "ts_dt_utc": ts_dt, "symbol": sym,
                "n_trades_1s": n,
                "sum_qty_1s": sum_qty,
                "vwap_1s": vwap,
                "buy_qty_1s": buy_qty,
                "sell_qty_1s": sell_qty,
                "buy_notional_1s": buy_notional,
                "sell_notional_1s": sell_notional,
            })

            # ----- L2 depth5 -----
            l2 = snap.get("l2") or {}
            bids = l2.get("bids") or []
            asks = l2.get("asks") or []

            row = {"ts_unix_ms": ts_ms, "ts_dt_utc": ts_dt, "symbol": sym}
            bid_sum = 0.0
            ask_sum = 0.0

            for i in range(5):
                if i < len(bids):
                    bp, bq = bids[i]
                    row[f"bid{i+1}_px"] = bp
                    row[f"bid{i+1}_qty"] = bq
                    bid_sum += float(bq)
                else:
                    row[f"bid{i+1}_px"] = None
                    row[f"bid{i+1}_qty"] = None

            for i in range(5):
                if i < len(asks):
                    ap, aq = asks[i]
                    row[f"ask{i+1}_px"] = ap
                    row[f"ask{i+1}_qty"] = aq
                    ask_sum += float(aq)
                else:
                    row[f"ask{i+1}_px"] = None
                    row[f"ask{i+1}_qty"] = None

            denom = bid_sum + ask_sum
            row["obi5"] = ((bid_sum - ask_sum) / denom) if denom > 0 else None
            l2_rows.append(row)

        self.writers.bars.append_many(bars_rows)
        self.writers.bbo.append_many(bbo_rows)
        self.writers.funding.append_many(funding_rows)
        self.writers.trades_1s.append_many(trades_rows)
        self.writers.depth5.append_many(l2_rows)

    def close(self) -> None:
        self.writers.bars.close()
        self.writers.bbo.close()
        self.writers.funding.close()
        self.writers.trades_1s.close()
        self.writers.depth5.close()