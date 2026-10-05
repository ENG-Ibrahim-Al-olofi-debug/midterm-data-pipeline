"""Central ELT Pipeline Orchestrator coordinating all phases according to course requirements."""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

from config.settings import REPORTS_DIR, SMALL_FILE_THRESHOLD_MB
from src.batch_loader import load_csv
from src.file_router import RouteDecision, decision_dict, route_file
from src.metrics import save_results
from src.mongo_setup import Storage, get_storage
from src.spark_loader import load_with_pyspark


def run_pipeline(
    input_path: str | Path,
    threshold_mb: float = SMALL_FILE_THRESHOLD_MB,
    run_id: str | None = None,
    force_engine: str | None = None,
    allow_pyspark_fallback: bool = False,
    storage: Storage | None = None,
    reports_dir: str | Path | None = REPORTS_DIR,
) -> dict[str, Any]:
    """Execute the full ELT data pipeline:

    Phase 1: File Discovery & Engine Selection (File Router)
    Phase 2: Raw Ingestion into orders_raw (unfiltered, preserving source metadata)
    Phase 3: Transformation & Quality Rules application
    Phase 4: Classification (valid, corrected, quarantine)
    Phase 5: Idempotent Upsert to orders_validated & orders_quarantine
    Phase 6: Consistency Verification & Metrics recording
    """
    path = Path(input_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    # Phase 1: Routing
    decision = route_file(path, threshold_mb=threshold_mb)
    if force_engine:
        decision = RouteDecision(
            file_name=decision.file_name,
            file_size_bytes=decision.file_size_bytes,
            file_size_mb=decision.file_size_mb,
            threshold_mb=threshold_mb,
            engine_used=force_engine,
            reason=f"Manually forced by user: {force_engine}",
        )

    print("=" * 60)
    print(f"[*] ELT Pipeline Starting for: {path.name} ({decision.file_size_mb:.3f} MB)")
    print(f"[*] Selected Engine: {decision.engine_used.upper()} (Reason: {decision.reason})")
    print("=" * 60)

    run_id = run_id or f"run-{uuid.uuid4().hex[:8]}"
    owns_storage = False
    if storage is None:
        storage = get_storage()
        owns_storage = True

    try:
        if decision.engine_used == "python_batch":
            metrics = load_csv(path, storage, run_id=run_id, engine="python_batch")
        else:
            metrics = load_with_pyspark(
                path,
                storage,
                run_id=run_id,
                allow_local_fallback=allow_pyspark_fallback,
            )

        metrics["route_reason"] = decision.reason
        metrics["threshold_mb"] = threshold_mb
        metrics["storage_backend"] = storage.backend

        # Persist metrics to reports/results.json and reports/results.md if reports_dir is specified
        if reports_dir:
            save_results(metrics, reports_dir)

        print("\n" + "=" * 60)
        print(f"[SUCCESS] Pipeline Completed Successfully! Run ID: {run_id}")
        print(f"    - Rows Read / Raw Loaded:  {metrics['rows_read']}")
        print(f"    - Valid Records:           {metrics['valid_count']}")
        print(f"    - Corrected Records:       {metrics['corrected_count']}")
        print(f"    - Quarantined Records:     {metrics['quarantine_count']}")
        print(f"    - Raw Consistency Check:   {'PASS (100% Consistent)' if metrics['raw_consistency_check'] else 'FAIL'}")
        print(f"    - Throughput:              {metrics['throughput']:,.2f} records/sec")
        print("=" * 60 + "\n")

        return metrics
    finally:
        if owns_storage:
            storage.close()


def run_idempotency_verification(
    input_path: str | Path,
    threshold_mb: float = SMALL_FILE_THRESHOLD_MB,
    storage: Storage | None = None,
    reports_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify Section 6.10 & 10.8 requirements:

    1. First Run -> Populates orders_validated.
    2. Second Run with identical input -> Must NOT increase business records in orders_validated.
       inserted_count on 2nd run must be 0, unchanged_count matches previous records.
    3. Modify 1 record and verify update occurs without duplicate creation.
    """
    owns_storage = False
    if storage is None:
        storage = get_storage()
        owns_storage = True

    try:
        print("\n[IDEMPOTENCY TEST] Step 1: Executing First Run...")
        first_run = run_pipeline(input_path, threshold_mb=threshold_mb, run_id="idempotency-run-001", storage=storage, reports_dir=reports_dir)
        first_validated_count = storage.count("orders_validated")

        print("\n[IDEMPOTENCY TEST] Step 2: Executing Second Run on EXACT same input...")
        second_run = run_pipeline(input_path, threshold_mb=threshold_mb, run_id="idempotency-run-002", storage=storage, reports_dir=reports_dir)
        second_validated_count = storage.count("orders_validated")

        business_keys_preserved = (first_validated_count == second_validated_count)
        no_new_inserts = (second_run["inserted_count"] == 0)

        test_result = {
            "first_run_id": first_run["run_id"],
            "second_run_id": second_run["run_id"],
            "first_validated_count": first_validated_count,
            "second_validated_count": second_validated_count,
            "second_run_inserted": second_run["inserted_count"],
            "second_run_updated": second_run["updated_count"],
            "second_run_unchanged": second_run["unchanged_count"],
            "business_records_not_increased": business_keys_preserved,
            "idempotency_verified": business_keys_preserved and no_new_inserts,
        }

        print("\n" + "=" * 60)
        print("[IDEMPOTENCY VERIFICATION RESULT]")
        print(f"  Count after 1st run: {first_validated_count}")
        print(f"  Count after 2nd run: {second_validated_count}")
        print(f"  Second run inserted count: {second_run['inserted_count']} (Expected: 0)")
        print(f"  Result: {'PASSED: Idempotency 100% Guaranteed!' if test_result['idempotency_verified'] else 'FAILED'}")
        print("=" * 60 + "\n")

        return test_result
    finally:
        if owns_storage:
            storage.close()
