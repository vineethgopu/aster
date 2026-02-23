#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import smtplib
import subprocess
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from aster.rest_api import Client as AsterRestClient
from dotenv import load_dotenv


def _to_bool(v: str) -> bool:
    return str(v).strip().lower() == "true"


def _load_runtime_env() -> None:
    # Allow direct CLI runs (outside systemd) to pick up VM runtime env.
    env_file = os.getenv("ASTER_ENV_FILE", "/opt/aster/deploy/gce/aster.env")
    p = Path(env_file)
    if p.exists():
        load_dotenv(dotenv_path=p, override=False)


def _env_bool(primary_key: str, aliases: Tuple[str, ...] = (), default: str = "false") -> bool:
    for key in (primary_key, *aliases):
        raw = os.getenv(key)
        if raw is not None and str(raw).strip() != "":
            return _to_bool(raw)
    return _to_bool(default)


def _run_cmd(cmd: List[str]) -> str:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, check=False)
        out = (p.stdout or "").strip()
        err = (p.stderr or "").strip()
        if p.returncode != 0 and err:
            return f"ERROR({p.returncode}): {err}"
        return out or err
    except Exception as e:
        return f"ERROR: {e}"


def _service_summary() -> str:
    active = _run_cmd(["systemctl", "is-active", "aster"])
    status_line = _run_cmd(
        ["systemctl", "show", "aster", "--property=ActiveState,SubState,Result,ExecMainStatus", "--no-page"]
    )
    recent_done = _run_cmd(
        ["journalctl", "-u", "aster", "--since", "26 hours ago", "--no-pager", "-o", "cat"]
    )
    done_count = recent_done.count("done:")
    return (
        f"aster service active={active}\n"
        f"{status_line}\n"
        f"completed_runs_last_26h={done_count}"
    )


def _parse_resp_rows(resp: Any) -> List[Dict[str, Any]]:
    if isinstance(resp, dict):
        data = resp.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        return [resp]
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]
    return []


def _read_secret_file(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def _prod_trade_summary() -> str:
    secret_dir = Path(os.getenv("ASTER_SECRET_DIR", "/opt/aster/.secrets"))
    api_key_path = secret_dir / "api_key"
    api_secret_path = secret_dir / "api_secret"
    if not api_key_path.exists() or not api_secret_path.exists():
        return f"Missing secret files under {secret_dir}. Skipping trade summary."

    symbols = [x.strip().upper() for x in os.getenv("ASTER_SYMBOLS", "ETHUSDT").split(",") if x.strip()]
    if not symbols:
        return "No ASTER_SYMBOLS configured."

    api_key = _read_secret_file(api_key_path)
    api_secret = _read_secret_file(api_secret_path)
    rest = AsterRestClient(api_key, api_secret, base_url="https://fapi.asterdex.com")

    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    lines = [f"trade_window_utc=[{day_start.isoformat()} -> {now.isoformat()}]"]
    header = "symbol,trades,total_realized_pnl,avg_pnl_per_trade,avg_fee_per_trade"
    lines.append(header)

    for sym in symbols:
        try:
            resp = rest.get_account_trades(symbol=sym, startTime=start_ms, endTime=end_ms, recvWindow=6000)
            rows = _parse_resp_rows(resp)
        except Exception as e:
            lines.append(f"{sym},ERROR,{e},,")
            continue

        n = len(rows)
        if n == 0:
            lines.append(f"{sym},0,0.0,0.0,0.0")
            continue

        rpnl = []
        commissions = []
        for r in rows:
            try:
                rpnl.append(float(r.get("realizedPnl", 0.0)))
            except Exception:
                rpnl.append(0.0)
            try:
                commissions.append(abs(float(r.get("commission", 0.0))))
            except Exception:
                commissions.append(0.0)

        total_rpnl = float(sum(rpnl))
        avg_rpnl = total_rpnl / n
        avg_fee = float(sum(commissions)) / n
        lines.append(f"{sym},{n},{total_rpnl:.8f},{avg_rpnl:.8f},{avg_fee:.8f}")
    return "\n".join(lines)


def _runtime_issue_summary() -> str:
    raw = _run_cmd(["journalctl", "-u", "aster", "--since", "24 hours ago", "--no-pager", "-o", "cat"])
    if raw.startswith("ERROR("):
        return raw

    pats: List[Tuple[str, str]] = [
        ("error_lines", r"\bERROR\b"),
        ("warning_lines", r"\bWARNING\b"),
        ("timeouts", r"timeout|timed out|handshake timeout"),
        ("conn_closed", r"connection closed|code:\s*1006|Lost connection"),
        ("tracebacks", r"Traceback \(most recent call last\)"),
        ("client_errors", r"ClientError"),
    ]
    out = []
    for name, pat in pats:
        cnt = len(re.findall(pat, raw, flags=re.IGNORECASE))
        out.append(f"{name}={cnt}")
    return "\n".join(out)


def _top10_backtest_text(ranked_csv: Path) -> str:
    if not ranked_csv.exists():
        return f"Backtest ranked file missing: {ranked_csv}"

    try:
        df = pd.read_csv(ranked_csv)
    except Exception as e:
        return f"Failed to read ranked CSV {ranked_csv}: {e}"

    required = {"symbol", "config_id", "total_pnl"}
    if not required.issubset(set(df.columns)):
        return f"Ranked CSV missing required columns {sorted(required)}: {ranked_csv}"

    blocks: List[str] = []
    for sym in sorted(df["symbol"].dropna().astype(str).unique()):
        sym_df = df[df["symbol"].astype(str) == sym].copy()
        sym_df = sym_df.sort_values("total_pnl", ascending=False).head(10)
        blocks.append(f"Top 10 configs for {sym}:")
        keep_cols = [c for c in [
            "config_id", "total_pnl", "total_return", "n_trades", "win_rate", "sharpe", "sortino",
            "max_drawdown", "avg_trade_pnl", "avg_win", "avg_loss", "avg_hold_bars",
            "k", "T", "n", "V", "tp_bps", "sl_bps",
            "activation_bps", "activation_buffer_bps", "callback_bps", "min_tp_gap_bps",
            "spread_max", "funding_max"
        ] if c in sym_df.columns]
        blocks.append(sym_df[keep_cols].to_string(index=False))
        blocks.append("")
    return "\n".join(blocks).strip()


def _send_email(subject_prefix: str, body: str, recipients_env_key: str) -> None:
    smtp_host = os.getenv("ASTER_EMAIL_SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("ASTER_EMAIL_SMTP_PORT", "587"))
    smtp_user = os.getenv("ASTER_EMAIL_SMTP_USER", "").strip()
    smtp_pass = os.getenv("ASTER_EMAIL_SMTP_PASS", "").strip()
    sender = os.getenv("ASTER_EMAIL_FROM", smtp_user).strip()
    recipients_raw = os.getenv(recipients_env_key, "").strip()
    recipients = [x.strip() for x in recipients_raw.split(",") if x.strip()]

    if not (smtp_host and smtp_user and smtp_pass and sender and recipients):
        raise ValueError(
            f"Email not configured. Set SMTP vars + recipients ({recipients_env_key})."
        )

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    msg = EmailMessage()
    msg["Subject"] = f"{subject_prefix} ({now_utc})"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["production", "backtest"], default="production")
    args = parser.parse_args()

    _load_runtime_env()
    enabled_key = "ASTER_EMAIL_PROD_ENABLE" if args.mode == "production" else "ASTER_EMAIL_BACKTEST_ENABLE"
    enabled = _env_bool(enabled_key, aliases=(f"{enabled_key}D",), default="false")
    if not enabled:
        print(f"[EMAIL] {enabled_key}=false; skipping report.")
        return

    if args.mode == "production":
        body = (
            "Aster Daily Production Report\n\n"
            "=== Service Status ===\n"
            f"{_service_summary()}\n\n"
            "=== Trade Summary (per symbol, today UTC) ===\n"
            f"{_prod_trade_summary()}\n\n"
            "=== Runtime Issues (last 24h) ===\n"
            f"{_runtime_issue_summary()}\n"
        )
        _send_email("Aster Production Report", body, recipients_env_key="ASTER_EMAIL_TO_PROD")
        print("[EMAIL] Production report sent.")
    else:
        ranked_csv = Path(os.getenv("ASTER_BACKTEST_RANKED_CSV", "/opt/aster/backtest/results/ranked_metrics.csv"))
        backtest_status = _run_cmd(["systemctl", "show", "aster-backtest.service", "--property=ActiveState,SubState,Result,ExecMainStatus", "--no-page"])
        body = (
            "Aster Weekly Backtest Report\n\n"
            "=== Backtest Service Status ===\n"
            f"{backtest_status}\n\n"
            "=== Top 10 Backtest Configs By Symbol ===\n"
            f"{_top10_backtest_text(ranked_csv)}\n"
        )
        _send_email("Aster Backtest Report", body, recipients_env_key="ASTER_EMAIL_TO_BACKTEST")
        print("[EMAIL] Backtest report sent.")


if __name__ == "__main__":
    main()
