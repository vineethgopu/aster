#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


TABLE_TO_PREFIX: Dict[str, str] = {
    "backtest_config": "backtest_config",
    "backtest_results": "backtest_results",
}


def _utc_today_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def _is_symbol_dated_file(path: Path, prefix: str, run_date: str) -> bool:
    stem = path.stem
    if not stem.endswith(f"_{run_date}"):
        return False
    parts = stem.split("_")
    # Expected:
    # - backtest_config_{SYMBOL}_{YYYYMMDD}
    # - backtest_results_{SYMBOL}_{YYYYMMDD}
    if prefix == "backtest_config":
        return len(parts) == 4 and parts[0] == "backtest" and parts[1] == "config" and parts[3] == run_date
    if prefix == "backtest_results":
        return len(parts) == 4 and parts[0] == "backtest" and parts[1] == "results" and parts[3] == run_date
    return False


def _find_symbol_files(results_dir: Path, prefix: str, run_date: str) -> List[Path]:
    out: List[Path] = []
    for p in sorted(results_dir.glob(f"{prefix}_*_{run_date}.csv")):
        if _is_symbol_dated_file(p, prefix=prefix, run_date=run_date):
            out.append(p)
    return out


def _get_table_or_raise(client: bigquery.Client, table_id: str) -> bigquery.Table:
    try:
        return client.get_table(table_id)
    except NotFound as exc:
        raise RuntimeError(
            f"BigQuery table not found: {table_id}. "
            "Create it first in BigQuery."
        ) from exc


def _delete_partition_date(client: bigquery.Client, table_id: str, target_date: date) -> None:
    sql = f"DELETE FROM `{table_id}` WHERE date = @target_date"
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
        ]
    )
    client.query(sql, job_config=cfg).result()


def _load_dataframe_csv(client: bigquery.Client, table_id: str, df: pd.DataFrame) -> int:
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
        temp_path = Path(tmp.name)
    try:
        df.to_csv(temp_path, index=False)
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
        )
        with temp_path.open("rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=cfg)
        job.result()
        return int(job.output_rows or 0)
    finally:
        temp_path.unlink(missing_ok=True)


def _prepare_table_frame(results_dir: Path, table: str, run_date: str) -> Optional[pd.DataFrame]:
    prefix = TABLE_TO_PREFIX[table]
    files = _find_symbol_files(results_dir=results_dir, prefix=prefix, run_date=run_date)
    if not files:
        print(f"[SKIP] {table}: no symbol files for date={run_date}")
        return None
    frames = [pd.read_csv(p) for p in files if p.exists()]
    if not frames:
        print(f"[SKIP] {table}: files unreadable/empty")
        return None
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        print(f"[SKIP] {table}: empty after concat")
        return None
    # Backtest run date partition key.
    df["date"] = datetime.strptime(run_date, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"[PREP] {table}: files={len(files)} rows={len(df)}")
    return df


def run(
    *,
    project_id: str,
    dataset: str,
    run_date: str,
    results_dir: Path,
    location: str,
    dry_run: bool,
) -> None:
    target_date = _parse_yyyymmdd(run_date)
    prepared: Dict[str, pd.DataFrame] = {}
    for table in TABLE_TO_PREFIX.keys():
        df = _prepare_table_frame(results_dir=results_dir, table=table, run_date=run_date)
        if df is not None:
            prepared[table] = df

    if not prepared:
        print("[SUMMARY] no backtest outputs to upload")
        return

    if dry_run:
        for table, df in prepared.items():
            print(f"[DRY_RUN] table={project_id}.{dataset}.{table} rows={len(df)}")
        return

    client = bigquery.Client(project=project_id, location=(location or None))
    total_loaded = 0
    for table, df in prepared.items():
        table_id = f"{project_id}.{dataset}.{table}"
        _get_table_or_raise(client, table_id=table_id)
        _delete_partition_date(client, table_id=table_id, target_date=target_date)
        loaded = _load_dataframe_csv(client=client, table_id=table_id, df=df)
        total_loaded += loaded
        print(f"[DONE] table={table_id} loaded_rows={loaded}")

    print(f"[SUMMARY] total_rows_loaded={total_loaded} date={run_date} dataset={project_id}.{dataset}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load dated backtest outputs into BigQuery (delete+insert by date).")
    parser.add_argument("--project", type=str, default="")
    parser.add_argument("--dataset", type=str, default="aster")
    parser.add_argument("--date", type=str, default=_utc_today_yyyymmdd(), help="Run date in YYYYMMDD")
    parser.add_argument("--results_dir", type=str, default="/opt/aster/backtest/results")
    parser.add_argument("--location", type=str, default="")
    parser.add_argument("--dry_run", action="store_true")
    args = parser.parse_args()

    project_id = args.project.strip() or os.getenv("GOOGLE_CLOUD_PROJECT", "")
    if not project_id:
        if args.dry_run:
            project_id = "dry-run-project"
        else:
            project_id = bigquery.Client(location=(args.location.strip() or None)).project

    run(
        project_id=project_id,
        dataset=args.dataset.strip(),
        run_date=args.date.strip(),
        results_dir=Path(args.results_dir),
        location=args.location.strip(),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

