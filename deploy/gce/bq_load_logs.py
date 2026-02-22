#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery



LOG_TABLE_MAP: Dict[str, str] = {
    "kline": "kline",
    "bookTicker": "book_ticker",
    "markPrice": "mark_price",
    "aggTrade_1s": "agg_trade_1s",
    "depth5": "depth5",
}

DEFAULT_NUMERIC_SCALE_BY_TYPE: Dict[str, int] = {
    "NUMERIC": 9,
    "BIGNUMERIC": 38,
}


def _utc_today_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def _resolve_log_file(log_dir: Path, base_name: str, date_str: str) -> Optional[Path]:
    dated = log_dir / f"{base_name}_{date_str}.csv"
    if dated.exists():
        return dated
    legacy = log_dir / f"{base_name}.csv"
    if legacy.exists():
        return legacy
    return None


def _coerce_dataframe_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "k1_closed" in out.columns:
        out["k1_closed"] = out["k1_closed"].astype(str).str.lower().isin(["true", "1"])
    for col in out.columns:
        if col.endswith("_ms"):
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _append_time_parts(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if "ts_unix_ms" not in df.columns:
        raise ValueError("Input CSV is missing required column ts_unix_ms")
    ts_unix_ms = pd.to_numeric(df["ts_unix_ms"], errors="coerce")
    dt = pd.to_datetime(ts_unix_ms, unit="ms", utc=True, errors="coerce")
    valid = dt.notna()
    dropped = int((~valid).sum())

    out = df.loc[valid].copy()
    dt = dt.loc[valid]
    out["date"] = dt.dt.strftime("%Y-%m-%d")
    out["hour"] = dt.dt.hour.astype("int64")
    out["minute"] = dt.dt.minute.astype("int64")
    out["second"] = dt.dt.second.astype("int64")
    return out, dropped


def _schema_from_df(df: pd.DataFrame) -> List[Tuple[str, str]]:
    schema: List[Tuple[str, str]] = []
    for col in df.columns:
        if col == "date":
            field_type = "DATE"
        elif col in {"hour", "minute", "second"}:
            field_type = "INT64"
        elif pd.api.types.is_bool_dtype(df[col]):
            field_type = "BOOL"
        elif pd.api.types.is_integer_dtype(df[col]):
            field_type = "INT64"
        elif pd.api.types.is_float_dtype(df[col]):
            field_type = "FLOAT64"
        else:
            field_type = "STRING"
        schema.append((col, field_type))
    return schema


def _prepare_logframes(log_dir: Path, date_str: str) -> List[Tuple[str, str, pd.DataFrame]]:
    prepared: List[Tuple[str, str, pd.DataFrame]] = []
    for base_name, table_name in LOG_TABLE_MAP.items():
        csv_path = _resolve_log_file(log_dir=log_dir, base_name=base_name, date_str=date_str)
        if csv_path is None:
            print(f"[SKIP] {base_name}: no file found for {date_str} in {log_dir}")
            continue

        print(f"[LOAD] {base_name}: {csv_path}")
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"[SKIP] {base_name}: empty file")
            continue
        df = _coerce_dataframe_types(df)
        df, dropped = _append_time_parts(df)
        if dropped > 0:
            print(f"[WARN] {base_name}: dropped {dropped} rows with invalid ts_unix_ms")
        if df.empty:
            print(f"[SKIP] {base_name}: no rows after ts_unix_ms validation")
            continue
        prepared.append((base_name, table_name, df))
    return prepared


def _get_table_or_raise(client: bigquery.Client, table_id: str) -> bigquery.Table:
    try:
        return client.get_table(table_id)
    except NotFound as exc:
        raise RuntimeError(
            f"BigQuery table not found: {table_id}. "
            "Create dataset/table manually in GCP console before running loader."
        ) from exc


def _format_numeric_cell(value: float, scale: int) -> Optional[str]:
    if pd.isna(value):
        return None
    text = f"{float(value):.{scale}f}"
    text = text.rstrip("0").rstrip(".")
    return text if text else "0"


def _coerce_numeric_columns_for_table(
    df: pd.DataFrame,
    table: bigquery.Table,
) -> Tuple[pd.DataFrame, List[str]]:
    out = df.copy()
    rounded_cols: List[str] = []
    schema_by_name = {field.name: field for field in table.schema}

    for col in out.columns:
        field = schema_by_name.get(col)
        if field is None:
            continue

        field_type = field.field_type.upper()
        if field_type not in DEFAULT_NUMERIC_SCALE_BY_TYPE:
            continue

        field_scale = getattr(field, "scale", None)
        if field_scale is None:
            field_scale = field.to_api_repr().get("scale")
        scale = int(field_scale) if field_scale is not None else DEFAULT_NUMERIC_SCALE_BY_TYPE[field_type]
        numeric_vals = pd.to_numeric(out[col], errors="coerce").round(scale)
        out[col] = numeric_vals.map(lambda x, s=scale: _format_numeric_cell(x, s))
        rounded_cols.append(f"{col}(scale={scale})")

    return out, rounded_cols


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


def run(
    log_dir: Path,
    project_id: str,
    dataset_name: str,
    date_str: str,
    location: str,
    dry_run: bool,
) -> None:
    prepared = _prepare_logframes(log_dir=log_dir, date_str=date_str)
    if not prepared:
        print("[SUMMARY] no log files available for upload")
        return

    if dry_run:
        total_rows = 0
        for base_name, table_name, df in prepared:
            table_id = f"{project_id}.{dataset_name}.{table_name}"
            schema_pairs = _schema_from_df(df)
            print(f"[DRY_RUN] table={table_id} rows={len(df)}")
            print(f"[DRY_RUN] expected_columns={schema_pairs}")
            print("[DRY_RUN] expects table to already exist with compatible schema.")
            total_rows += len(df)
            print(f"[DONE] {base_name}: loaded_rows={len(df)} table={table_id}")
        print(f"[SUMMARY] total_rows_loaded={total_rows} date={date_str} dataset={project_id}.{dataset_name}")
        return

    client = bigquery.Client(project=project_id, location=(location or None))
    target_date = _parse_yyyymmdd(date_str)

    total_rows = 0
    for base_name, table_name, df in prepared:
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        table = _get_table_or_raise(client=client, table_id=table_id)
        df_for_load, rounded_cols = _coerce_numeric_columns_for_table(df=df, table=table)
        if rounded_cols:
            print(f"[INFO] {base_name}: normalized numeric precision for columns={rounded_cols}")
        _delete_partition_date(client=client, table_id=table_id, target_date=target_date)
        loaded = _load_dataframe_csv(client=client, table_id=table_id, df=df_for_load)
        total_rows += loaded
        print(f"[DONE] {base_name}: loaded_rows={loaded} table={table_id}")

    print(f"[SUMMARY] total_rows_loaded={total_rows} date={date_str} dataset={project_id}.{dataset_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch load daily Aster CSV logs into existing BigQuery tables.")
    parser.add_argument("--log_dir", type=str, default="/opt/aster/logs")
    parser.add_argument("--project", type=str, default="")
    parser.add_argument("--dataset", type=str, default="aster")
    parser.add_argument("--date", type=str, default=_utc_today_yyyymmdd(), help="UTC date in YYYYMMDD")
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
        log_dir=Path(args.log_dir),
        project_id=project_id,
        dataset_name=args.dataset.strip(),
        date_str=args.date.strip(),
        location=args.location.strip(),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
