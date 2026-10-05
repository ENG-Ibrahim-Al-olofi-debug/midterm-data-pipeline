"""Unit tests for record classification and quarantine error codes (Section 6.8)."""
from __future__ import annotations

from src.quality_rules import clean_and_classify


def test_clean_record_is_classified_valid():
    """A completely clean record must be classified as 'valid' with no corrections and no errors."""
    record = {
        "order_id": "ORD-100",
        "customer_id": "C-100",
        "order_date": "2025-02-01",
        "total_amount": "50000.0",
        "currency": "YER",
        "status": "مؤكد",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "items_json": '[{"sku":"A1","qty":1,"unit_price":50000.0,"total":50000.0}]',
    }
    result = clean_and_classify(record)
    assert result.status == "valid"
    assert len(result.corrections) == 0
    assert len(result.errors) == 0


def test_corrected_record_has_audit_trail():
    """A record with correctable flaws must have quality_status 'corrected' and proper audit trail."""
    record = {
        "order_id": "ORD-101",
        "customer_id": "C-101",
        "order_date": "01/02/2025",  # non-ISO date
        "total_amount": "٥٠٠٠",     # Arabic digits
        "customer_email": "user@@domain.com",  # repeated @
        "items_json": '[{"sku":"A","qty":1,"unit_price":5000,"total":5000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert len(result.corrections) >= 3
    assert result.record["order_date"] == "2025-02-01"
    assert result.record["total_amount"] == 5000.0
    assert result.record["customer_email"] == "user@domain.com"


def test_quarantine_missing_order_id():
    """Missing or empty order_id must be quarantined with MISSING_ORDER_ID."""
    result = clean_and_classify({
        "order_id": "",
        "customer_id": "C-1",
        "order_date": "2025-01-01",
        "total_amount": "100",
        "items_json": '[{"sku":"A","qty":1}]',
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "MISSING_ORDER_ID" for e in result.errors)


def test_quarantine_missing_customer_id():
    """Missing or empty customer_id must be quarantined with MISSING_CUSTOMER_ID."""
    result = clean_and_classify({
        "order_id": "ORD-1",
        "customer_id": "",
        "order_date": "2025-01-01",
        "total_amount": "100",
        "items_json": '[{"sku":"A","qty":1}]',
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "MISSING_CUSTOMER_ID" for e in result.errors)


def test_quarantine_invalid_impossible_date():
    """Unparseable or out-of-range date must be quarantined with INVALID_IMPOSSIBLE_DATE."""
    result = clean_and_classify({
        "order_id": "ORD-1",
        "customer_id": "C-1",
        "order_date": "bad-date-string",
        "total_amount": "100",
        "items_json": '[{"sku":"A","qty":1}]',
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "INVALID_IMPOSSIBLE_DATE" for e in result.errors)


def test_quarantine_corrupted_items_json():
    """Malformed or invalid JSON string must trigger CORRUPTED_ITEMS_JSON."""
    result = clean_and_classify({
        "order_id": "ORD-1",
        "customer_id": "C-1",
        "order_date": "2025-01-01",
        "total_amount": "100",
        "items_json": "{not valid json}",
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "CORRUPTED_ITEMS_JSON" for e in result.errors)


def test_quarantine_empty_items():
    """Empty list of items must trigger EMPTY_ITEMS."""
    result = clean_and_classify({
        "order_id": "ORD-1",
        "customer_id": "C-1",
        "order_date": "2025-01-01",
        "total_amount": "100",
        "items_json": "[]",
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "EMPTY_ITEMS" for e in result.errors)


def test_quarantine_unknown_price():
    """Price that cannot be inferred must trigger UNKNOWN_PRICE."""
    result = clean_and_classify({
        "order_id": "ORD-1",
        "customer_id": "C-1",
        "order_date": "2025-01-01",
        "total_amount": "???",
        "items_json": "not-json",  # cannot recalculate
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "UNKNOWN_PRICE" for e in result.errors)


def test_quarantine_ambiguous_negative_value():
    """Negative amount or item quantity must trigger AMBIGUOUS_NEGATIVE_VALUE."""
    result = clean_and_classify({
        "order_id": "ORD-1",
        "customer_id": "C-1",
        "order_date": "2025-01-01",
        "total_amount": "-500",
        "items_json": '[{"sku":"A","qty":1,"unit_price":100,"total":100}]',
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "AMBIGUOUS_NEGATIVE_VALUE" for e in result.errors)


def test_quarantine_multiple_conflicting_errors():
    """Records with 3 or more errors must include MULTIPLE_CONFLICTING_ERRORS."""
    result = clean_and_classify({
        "order_id": "",
        "customer_id": "",
        "order_date": "bad",
        "total_amount": "???",
        "items_json": "bad",
    })
    assert result.status == "quarantine"
    assert any(e["code"] == "MULTIPLE_CONFLICTING_ERRORS" for e in result.errors)
