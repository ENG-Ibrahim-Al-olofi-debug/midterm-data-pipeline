"""PySpark loader for large files using Spark DataFrame API and distributed partition processing."""
from __future__ import annotations

import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from config.settings import MONGO_DB, MONGO_URI, setup_spark_windows_env
from src.mongo_setup import JsonStorage, MongoStorage, get_storage

Storage = JsonStorage | MongoStorage


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_fixed_schema():
    """Return explicit fixed Schema as required by Section 6.4 (all fields StringType for Raw)."""
    from pyspark.sql.types import StringType, StructField, StructType
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True),
    ])


def _process_partition(
    partition_index: int,
    iterator: Iterator[Any],
    run_id: str,
    source_file: str,
) -> Iterator[dict[str, Any]]:
    """Distributed partition worker: ingests raw records in streaming chunks, cleans, classifies, and loads."""
    from src.mongo_setup import get_storage
    from src.quality_rules import clean_and_classify

    storage = get_storage()
    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    error_counts: dict[str, int] = {}
    correction_counts: dict[str, int] = {}
    rows_in_partition = 0

    chunk: list[tuple[int, dict[str, Any]]] = []

    def _flush_chunk(items: list[tuple[int, dict[str, Any]]]) -> None:
        nonlocal valid_count, corrected_count, quarantine_count
        nonlocal inserted_count, updated_count, unchanged_count, correction_counts

        if not items:
            return

        # 1. Ingest Raw records into orders_raw
        raw_docs = [
            {
                "run_id": run_id,
                "source_file": source_file,
                "source_row_number": row_num,
                "ingested_at": _now(),
                "engine_used": "pyspark",
                "raw_record": raw_dict,
                "_storage_key": f"{run_id}:{row_num}",
            }
            for row_num, raw_dict in items
        ]
        storage.insert_many("orders_raw", raw_docs)

        # 2. Transform, Classify & Upsert
        for row_num, raw_dict in items:
            raw_key = f"{run_id}:{row_num}"
            result = clean_and_classify(raw_dict)

            record_payload = {
                **result.record,
                "run_id": run_id,
                "source_file": source_file,
                "source_row_number": row_num,
                "processed_at": _now(),
                "quality_status": result.status,
                "corrections": result.corrections,
                "raw_record": raw_dict,
            }

            if result.status == "quarantine":
                quarantine_count += 1
                error_codes = [str(e["code"]) for e in result.errors]
                for code in error_codes:
                    error_counts[code] = error_counts.get(code, 0) + 1

                quarantine_doc = {
                    **record_payload,
                    "error_codes": error_codes,
                    "error_details": result.errors,
                }
                storage.upsert("orders_quarantine", raw_key, quarantine_doc)
            else:
                if result.status == "corrected":
                    corrected_count += 1
                    for corr in result.corrections:
                        rule = str(corr.get("rule_code"))
                        correction_counts[rule] = correction_counts.get(rule, 0) + 1
                else:
                    valid_count += 1

                order_id = str(result.record["order_id"])
                op = storage.upsert("orders_validated", order_id, record_payload)
                if op == "inserted":
                    inserted_count += 1
                elif op == "updated":
                    updated_count += 1
                else:
                    unchanged_count += 1

    try:
        for idx, row in enumerate(iterator):
            rows_in_partition += 1
            row_number = partition_index * 10_000_000 + idx + 2
            raw_dict = row.asDict() if hasattr(row, "asDict") else dict(row)
            chunk.append((row_number, raw_dict))

            if len(chunk) >= 1000:
                _flush_chunk(chunk)
                chunk = []

        if chunk:
            _flush_chunk(chunk)
            chunk = []

        storage.close()

        yield {
            "partition": partition_index,
            "rows": rows_in_partition,
            "valid_count": valid_count,
            "corrected_count": corrected_count,
            "quarantine_count": quarantine_count,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "error_counts": error_counts,
            "correction_counts": correction_counts,
        }
    except Exception:
        storage.close()
        raise


def load_with_pyspark(
    path: str | Path,
    storage: Storage,
    run_id: str | None = None,
    allow_local_fallback: bool = False,
) -> dict[str, Any]:
    """Load large file using Apache Spark DataFrame API with fixed Schema and distributed execution."""
    setup_spark_windows_env()
    source_path = Path(path).resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    run_id = run_id or uuid.uuid4().hex

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        if allow_local_fallback:
            from src.batch_loader import load_csv
            metrics = load_csv(source_path, storage, run_id=run_id, engine="pyspark_fallback")
            metrics["engine_note"] = "Local fallback used because PySpark is not installed."
            return metrics
        raise RuntimeError("PySpark is required for the large-file path.") from exc

    started = time.perf_counter()

    spark = (
        SparkSession.builder
        .appName("MidtermHybridDataPipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.ui.enabled", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    try:
        fixed_schema = _get_fixed_schema()
        # Read with fixed schema and no inferSchema as specified in Section 6.4
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("multiLine", "true")
            .schema(fixed_schema)
            .load(str(source_path))
        )

        num_partitions = df.rdd.getNumPartitions()
        print(f"[PySpark] Loaded DataFrame with {num_partitions} partition(s). Execution Plan:")
        df.explain(False)

        # Distribute partition processing across workers
        source_name = source_path.name
        partition_results = df.rdd.mapPartitionsWithIndex(
            lambda idx, it: _process_partition(idx, it, run_id, source_name)
        ).collect()

        total_rows = sum(r["rows"] for r in partition_results)
        valid_count = sum(r["valid_count"] for r in partition_results)
        corrected_count = sum(r["corrected_count"] for r in partition_results)
        quarantine_count = sum(r["quarantine_count"] for r in partition_results)
        inserted_count = sum(r["inserted_count"] for r in partition_results)
        updated_count = sum(r["updated_count"] for r in partition_results)
        unchanged_count = sum(r["unchanged_count"] for r in partition_results)

        total_error_counts: dict[str, int] = {}
        total_correction_counts: dict[str, int] = {}
        for r in partition_results:
            for code, count in r["error_counts"].items():
                total_error_counts[code] = total_error_counts.get(code, 0) + count
            for code, count in r.get("correction_counts", {}).items():
                total_correction_counts[code] = total_correction_counts.get(code, 0) + count

        elapsed = time.perf_counter() - started
        throughput = round(total_rows / elapsed, 2) if elapsed > 0 else 0

        # Reload storage to reflect written records
        if hasattr(storage, "_load"):
            storage._load()

        return {
            "run_id": run_id,
            "file_name": source_name,
            "file_size_mb": round(source_path.stat().st_size / (1024 * 1024), 6),
            "engine_used": "pyspark",
            "rows_read": total_rows,
            "raw_loaded": total_rows,
            "valid_count": valid_count,
            "corrected_count": corrected_count,
            "quarantine_count": quarantine_count,
            "elapsed_seconds": round(elapsed, 4),
            "throughput": throughput,
            "batch_size": None,
            "partitions": num_partitions,
            "error_case_counts": total_error_counts,
            "correction_case_counts": total_correction_counts,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "raw_consistency_check": (total_rows == valid_count + corrected_count + quarantine_count),
            "storage_backend": storage.backend,
        }
    finally:
        spark.stop()
