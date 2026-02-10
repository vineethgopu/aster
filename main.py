# main.py
from __future__ import annotations

from client import AsterClient
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--symbols', '-s', type=str, default='BTCUSDT')
    parser.add_argument('--log_dir', '-l', type=str, default='./logs')
    parser.add_argument('--poll_time', '-t', type=int, default=600)

    args = parser.parse_args()
    symbols = args.symbols.split(',')
    log_dir = args.log_dir
    poll_time = args.poll_time

    c = AsterClient(symbols=symbols, log_dir=log_dir)

    result = c.run(run_seconds=poll_time)
    print("done:", {k: v for k, v in result.items() if k != "startup_snapshot"})
    print(f"logs written to {log_dir}")
