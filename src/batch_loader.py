"""Streaming batch CSV loader for small files following ELT principles."""
from __future__ import annotations

import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import BATCH_SIZE
from src.mongo_setup import JsonStorage, MongoStorage
from src.quality_rules import clean_and_classify

Storage = JsonStorage | MongoStorage


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _format_raw_doc(raw: dict[str, Any], run_id: str, source_file: str, row_number: int, engine: str) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "source_file": source_file,
        "source_row_number": row_number,
        "ingested_at": _now(),
        "engine_used": engine,
        "raw_record": raw,
        "_storage_key": f"{run_id}:{row_number}",
    }


def load_csv(
    path: str | Path,
    storage: Storage,
    run_id: str | None = None,
    engine: str = "python_batch",
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    """Execute streaming batch load into orders_raw, then transform & load to validated/quarantine."""
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"Input file not found: {source}")

    run_id = run_id or uuid.uuid4().hex
    started = time.perf_counter()

    rows_read = 0
    raw_loaded = 0
    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    error_case_counts: dict[str, int] = {}
    correction_case_counts: dict[str, int] = {}

    batch_number = 0
    current_batch_raw: list[tuple[int, dict[str, Any]]] = []

    def _flush_batch(batch_items: list[tuple[int, dict[str, Any]]]) -> None:
        nonlocal batch_number, raw_loaded, valid_count, corrected_count, quarantine_count
        nonlocal inserted_count, updated_count, unchanged_count, correction_case_counts

        if not batch_items:
            return

        batch_number += 1
        batch_start = time.perf_counter()

        # Phase 1 of ELT: Load Raw as received without filtering
        raw_docs = [
            _format_raw_doc(raw, run_id, source.name, row_num, engine)
            for row_num, raw in batch_items
        ]
        try:
            inserted_raw = storage.insert_many("orders_raw", raw_docs)
            raw_loaded += len(batch_items)
        except Exception as exc:
            print(f"[ERROR] Failed to insert raw batch #{batch_number}: {exc}", file=sys.stderr)
            raise

        # Phase 2 of ELT: Clean, Classify, and Load to Validated / Quarantine
        for row_num, raw in batch_items:
            result = clean_and_classify(raw)
            raw_key = f"{run_id}:{row_num}"

            record_payload = {
                **result.record,
                "run_id": run_id,
                "source_file": source.name,
                "source_row_number": row_num,
                "processed_at": _now(),
                "quality_status": result.status,
                "corrections": result.corrections,
                "raw_record": raw,
            }

            if result.status == "quarantine":
                quarantine_count += 1
                error_codes = [str(e["code"]) for e in result.errors]
                for code in error_codes:
                    error_case_counts[code] = error_case_counts.get(code, 0) + 1

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
                        correction_case_counts[rule] = correction_case_counts.get(rule, 0) + 1
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

        batch_elapsed = time.perf_counter() - batch_start
        batch_rate = round(len(batch_items) / batch_elapsed, 2) if batch_elapsed > 0 else 0
        print(
            f"[Batch #{batch_number:03d}] Ingested & processed {len(batch_items)} records "
            f"in {batch_elapsed:.3f}s ({batch_rate:,.1f} rec/s)"
        )

    # Streaming read using csv.DictReader (no full file loaded into memory)
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, raw in enumerate(reader, start=2):
            rows_read += 1
            current_batch_raw.append((row_number, raw))
            if len(current_batch_raw) >= batch_size:
                _flush_batch(current_batch_raw)
                current_batch_raw = []

        if current_batch_raw:
            _flush_batch(current_batch_raw)
            current_batch_raw = []

    elapsed = time.perf_counter() - started
    throughput = round(rows_read / elapsed, 2) if elapsed > 0 else 0

    return {
        "run_id": run_id,
        "file_name": source.name,
        "file_size_mb": round(source.stat().st_size / (1024 * 1024), 6),
        "engine_used": engine,
        "rows_read": rows_read,
        "raw_loaded": raw_loaded,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "elapsed_seconds": round(elapsed, 4),
        "throughput": throughput,
        "batch_size": batch_size,
        "partitions": None,
        "error_case_counts": error_case_counts,
        "correction_case_counts": correction_case_counts,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "raw_consistency_check": (raw_loaded == valid_count + corrected_count + quarantine_count),
        "storage_backend": storage.backend,
    }
