"""Integration tests for the complete ELT Pipeline, router, consistency, and idempotency."""
from __future__ import annotations

import csv
from pathlib import Path

from src.batch_loader import load_csv
from src.elt_pipeline import run_idempotency_verification, run_pipeline
from src.file_router import route_file
from src.incremental_loader import run_path_b_demonstration
from src.mongo_setup import JsonStorage


def _create_sample_csv(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["order_id", "customer_id", "order_date", "total_amount", "currency", "status", "customer_phone", "customer_email", "items_json"],
        )
        writer.writeheader()
        # 1. Clean valid record
        writer.writerow({
            "order_id": "ORD-T1", "customer_id": "C1", "order_date": "2025-01-15",
            "total_amount": "15000", "currency": "YER", "status": "مؤكد",
            "customer_phone": "771234567", "customer_email": "c1@test.com",
            "items_json": '[{"sku":"A","qty":1,"unit_price":15000,"total":15000}]',
        })
        # 2. Correctable record (Arabic digits + repeated @)
        writer.writerow({
            "order_id": "ORD-T2", "customer_id": "C2", "order_date": "15/01/2025",
            "total_amount": "٢٥,٠٠٠", "currency": "ريال يمني", "status": "  مؤكد  ",
            "customer_phone": "+967 77 999 8888", "customer_email": "c2@@test..com",
            "items_json": '[{"sku":"B","qty":1,"unit_price":25000,"total":25000}]',
        })
        # 3. Quarantined record (missing customer_id, bad date)
        writer.writerow({
            "order_id": "ORD-T3", "customer_id": "", "order_date": "impossible-date",
            "total_amount": "100", "currency": "YER", "status": "مؤكد",
            "customer_phone": "123", "customer_email": "bad",
            "items_json": "corrupted",
        })


def test_file_router_threshold(tmp_path: Path):
    """File router must pick python_batch when size <= threshold and pyspark when size > threshold."""
    path = tmp_path / "test.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    assert route_file(path, threshold_mb=100.0).engine_used == "python_batch"
    assert route_file(path, threshold_mb=0.0).engine_used == "pyspark"


def test_elt_pipeline_consistency_and_counts(tmp_path: Path):
    """Section 6.11: run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count."""
    csv_file = tmp_path / "orders.csv"
    _create_sample_csv(csv_file)

    storage = JsonStorage(tmp_path / "store")
    metrics = load_csv(csv_file, storage, run_id="test_run_01")

    assert metrics["rows_read"] == 3
    assert metrics["raw_loaded"] == 3
    assert metrics["valid_count"] == 1
    assert metrics["corrected_count"] == 1
    assert metrics["quarantine_count"] == 1
    assert metrics["raw_consistency_check"] is True
    assert metrics["valid_count"] + metrics["corrected_count"] + metrics["quarantine_count"] == 3

    assert storage.count("orders_validated") == 2
    assert storage.count("orders_quarantine") == 1
    storage.close()


def test_pipeline_idempotency_verification(tmp_path: Path):
    """Section 6.10: Re-running exact same input must not increase business records in orders_validated."""
    csv_file = tmp_path / "orders.csv"
    _create_sample_csv(csv_file)

    storage = JsonStorage(tmp_path / "store_idemp")
    result = run_idempotency_verification(csv_file, storage=storage)

    assert result["idempotency_verified"] is True
    assert result["first_validated_count"] == result["second_validated_count"] == 2
    assert result["second_run_inserted"] == 0
    storage.close()


def test_path_b_incremental_demonstration(tmp_path: Path):
    """Section 7: Path B initial load, delta update+insert, and delta re-run idempotency."""
    storage = JsonStorage(tmp_path / "store_inc")
    res = run_path_b_demonstration(storage=storage)

    assert res["idempotency_guaranteed"] is True
    assert res["experiment_3_delta_rerun"]["inserted_count"] == 0
    assert res["count_after_delta"] == res["count_after_rerun"]
    storage.close()
