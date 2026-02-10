# main.py
from __future__ import annotations

from client import AsterClient

if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT"]  # add more symbols if you want
    c = AsterClient(symbols=symbols, log_dir="./logs")

    result = c.run(run_seconds=3600)  # 1 hour
    print("done:", {k: v for k, v in result.items() if k != "startup_snapshot"})
    print("logs written to ./logs")
