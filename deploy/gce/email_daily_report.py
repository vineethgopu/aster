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

try:
    from google.cloud import bigquery
except Exception:  # pragma: no cover - optional runtime dependency.
    bigquery = None  # type: ignore[assignment]


BQ_LOG_TABLES: List[Tuple[str, str]] = [
    ("kline", "kline"),
    ("bookTicker", "book_ticker"),
    ("markPrice", "mark_price"),
    ("aggTrade_1s", "agg_trade_1s"),
    ("depth5", "depth5"),
    ("orders", "orders"),
]


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


def _resolve_email_smtp_pass() -> str:
    # SMTP password is read from the local secret file populated by
    # deploy/gce/fetch_secrets.sh using Secret Manager.
    secret_dir = Path(os.getenv("ASTER_SECRET_DIR", "/opt/aster/.secrets"))
    pass_file = secret_dir / "email_smtp_pass"
    if pass_file.exists():
        return _read_secret_file(pass_file)
    return ""


def _trade_duration_stats(start_ms: int, end_ms: int) -> Dict[str, Dict[str, float]]:
    log_dir = Path(os.getenv("ASTER_LOG_DIR", "/opt/aster/logs"))
    if not log_dir.exists():
        return {}

    day_start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")
    day_end = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")
    day_keys = {day_start, day_end}

    frames: List[pd.DataFrame] = []
    for day in sorted(day_keys):
        p = log_dir / f"orders_{day}.csv"
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if df.empty or "symbol" not in df.columns or "order_duration_s" not in df.columns:
            continue
        if "ts_unix_ms" in df.columns:
            ts = pd.to_numeric(df["ts_unix_ms"], errors="coerce")
            df = df[(ts >= start_ms) & (ts <= end_ms)]
        elif "exit_fill_time_ms" in df.columns:
            ts = pd.to_numeric(df["exit_fill_time_ms"], errors="coerce")
            df = df[(ts >= start_ms) & (ts <= end_ms)]
        elif "exit_exec_time_ms" in df.columns:
            # Backward compatibility for pre-rename files.
            ts = pd.to_numeric(df["exit_exec_time_ms"], errors="coerce")
            df = df[(ts >= start_ms) & (ts <= end_ms)]
        frames.append(df)

    if not frames:
        return {}

    all_df = pd.concat(frames, ignore_index=True)
    if all_df.empty:
        return {}
    all_df["order_duration_s"] = pd.to_numeric(all_df["order_duration_s"], errors="coerce")
    all_df = all_df.dropna(subset=["symbol", "order_duration_s"])
    if all_df.empty:
        return {}

    out: Dict[str, Dict[str, float]] = {}
    grouped = all_df.groupby(all_df["symbol"].astype(str))
    for sym, g in grouped:
        out[str(sym).upper()] = {
            "avg_order_duration_s": float(g["order_duration_s"].mean()),
            "n_closed_trades": float(len(g)),
        }
    return out


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
    duration_stats = _trade_duration_stats(start_ms=start_ms, end_ms=end_ms)

    lines = [f"trade_window_utc=[{day_start.isoformat()} -> {now.isoformat()}]"]
    header = "symbol,trades,total_realized_pnl,avg_pnl_per_trade,avg_fee_per_trade,avg_order_duration_s,n_closed_trades"
    lines.append(header)

    for sym in symbols:
        dur = duration_stats.get(sym, {})
        avg_dur = dur.get("avg_order_duration_s")
        n_closed = int(dur.get("n_closed_trades") or 0)
        try:
            resp = rest.get_account_trades(symbol=sym, startTime=start_ms, endTime=end_ms, recvWindow=6000)
            rows = _parse_resp_rows(resp)
        except Exception as e:
            lines.append(f"{sym},ERROR,{e},,,{avg_dur if avg_dur is not None else ''},{n_closed}")
            continue

        n = len(rows)
        if n == 0:
            lines.append(f"{sym},0,0.0,0.0,0.0,{avg_dur if avg_dur is not None else 0.0},{n_closed}")
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
        avg_dur_s = f"{avg_dur:.6f}" if avg_dur is not None else ""
        lines.append(f"{sym},{n},{total_rpnl:.8f},{avg_rpnl:.8f},{avg_fee:.8f},{avg_dur_s},{n_closed}")
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


def _bq_logs_summary() -> str:
    enabled = _env_bool("ASTER_BQ_ENABLE_DAILY_BATCH", default="false")
    if not enabled:
        return "ASTER_BQ_ENABLE_DAILY_BATCH=false"
    if bigquery is None:
        return "google-cloud-bigquery package is unavailable in runtime environment."

    project = (os.getenv("ASTER_BQ_PROJECT", "") or os.getenv("GOOGLE_CLOUD_PROJECT", "")).strip()
    dataset = os.getenv("ASTER_BQ_DATASET", "aster").strip()
    location = os.getenv("ASTER_BQ_LOCATION", "").strip()
    if not project:
        return "Missing ASTER_BQ_PROJECT/GOOGLE_CLOUD_PROJECT for BigQuery summary."
    if not dataset:
        return "Missing ASTER_BQ_DATASET for BigQuery summary."

    lines: List[str] = [
        f"project={project}",
        f"dataset={dataset}",
        f"location={location or '(default)'}",
    ]

    batch_status = _run_cmd(
        [
            "systemctl",
            "show",
            "aster-daily-stop.service",
            "--property=ActiveState,SubState,Result,ExecMainStatus",
            "--no-page",
        ]
    )
    lines.append(f"batch_service_status={batch_status}")

    batch_journal = _run_cmd(
        ["journalctl", "-u", "aster-daily-stop.service", "--since", "30 hours ago", "--no-pager", "-o", "cat"]
    )
    if not batch_journal.startswith("ERROR("):
        lines.append(f"batch_done_lines_last_30h={batch_journal.count('[DONE]')}")
        lines.append(f"batch_summary_lines_last_30h={batch_journal.count('[SUMMARY]')}")
        batch_error_lines = len(re.findall(r"\bERROR\b", batch_journal, flags=re.IGNORECASE))
        lines.append(f"batch_error_lines_last_30h={batch_error_lines}")

    try:
        client = bigquery.Client(project=project, location=(location or None))
    except Exception as e:
        lines.append(f"bigquery_client_error={e}")
        return "\n".join(lines)

    lines.append("table,max_partition_date,row_count")
    for _, table_name in BQ_LOG_TABLES:
        table_id = f"`{project}.{dataset}.{table_name}`"
        try:
            max_date_sql = f"SELECT MAX(date) AS max_date FROM {table_id}"
            max_date_job = client.query(max_date_sql)
            max_date_row = next(iter(max_date_job.result()), None)
            max_date = None if max_date_row is None else max_date_row["max_date"]
            if max_date is None:
                lines.append(f"{table_name},NONE,0")
                continue

            count_sql = f"SELECT COUNT(1) AS n FROM {table_id} WHERE date=@d"
            count_job = client.query(
                count_sql,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", max_date)]
                ),
            )
            count_row = next(iter(count_job.result()), None)
            n_rows = 0 if count_row is None else int(count_row["n"])
            lines.append(f"{table_name},{max_date},{n_rows}")
        except Exception as e:
            lines.append(f"{table_name},ERROR,{e}")

    return "\n".join(lines)


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
    smtp_pass = _resolve_email_smtp_pass()
    sender = smtp_user
    recipients_raw = os.getenv(recipients_env_key, "").strip()
    recipients = [x.strip() for x in recipients_raw.split(",") if x.strip()]

    if not (smtp_host and smtp_user and smtp_pass and recipients):
        raise ValueError(
            f"Email not configured. Set SMTP host/user + recipients ({recipients_env_key}) "
            "and ensure /opt/aster/.secrets/email_smtp_pass exists via fetch_secrets.sh."
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
            "=== BigQuery Log Batch Summary ===\n"
            f"{_bq_logs_summary()}\n\n"
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
            "=== BigQuery Log Batch Summary ===\n"
            f"{_bq_logs_summary()}\n\n"
            "=== Top 10 Backtest Configs By Symbol ===\n"
            f"{_top10_backtest_text(ranked_csv)}\n"
        )
        _send_email("Aster Backtest Report", body, recipients_env_key="ASTER_EMAIL_TO_BACKTEST")
        print("[EMAIL] Backtest report sent.")


if __name__ == "__main__":
    main()
