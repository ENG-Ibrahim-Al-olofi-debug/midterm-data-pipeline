"""Comprehensive unit tests for all 9 automated data cleaning rules (Section 6.6)."""
from __future__ import annotations

from src.quality_rules import clean_and_classify


def test_rule_1_arabic_digits_conversion():
    """Rule 1: Convert Arabic-Indic numerals and Arabic decimal comma to Latin numeric."""
    record = {
        "order_id": "ORD-1",
        "customer_id": "C-1",
        "total_amount": "٥٠٠٠٫٥",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"A","qty":1,"unit_price":5000.5,"total":5000.5}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["total_amount"] == 5000.5
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "ARABIC_DIGITS_CONVERT" in rule_codes


def test_rule_2_currency_normalization():
    """Rule 2: Strip currency text from amount and normalize currency symbol to YER."""
    record = {
        "order_id": "ORD-2",
        "customer_id": "C-2",
        "currency": "ريال يمني",
        "total_amount": "7500 ريال",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"A","qty":1,"unit_price":7500,"total":7500}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["currency"] == "YER"
    assert result.record["total_amount"] == 7500.0
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "CURRENCY_NORMALIZE" in rule_codes


def test_rule_3_thousands_separators():
    """Rule 3: Remove thousands separators (comma, Arabic comma) and convert to float."""
    record = {
        "order_id": "ORD-3",
        "customer_id": "C-3",
        "total_amount": "125,000.00",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"A","qty":1,"unit_price":125000,"total":125000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["total_amount"] == 125000.0
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "THOUSANDS_SEPARATOR_REMOVE" in rule_codes


def test_rule_4_price_in_words():
    """Rule 4: Convert known specific price words (e.g. ألفان, خمسة آلاف)."""
    record = {
        "order_id": "ORD-4",
        "customer_id": "C-4",
        "total_amount": "خمسة آلاف",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"A","qty":1,"unit_price":5000,"total":5000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["total_amount"] == 5000.0
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "PRICE_WORDS_CONVERT" in rule_codes


def test_rule_5_phone_normalization():
    """Rule 5: Strip whitespace and symbols, normalize to standard format."""
    record = {
        "order_id": "ORD-5",
        "customer_id": "C-5",
        "customer_phone": "+967 77 123 4567",
        "total_amount": "1000",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"A","qty":1,"unit_price":1000,"total":1000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["customer_phone"] == "+967771234567"
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "PHONE_NORMALIZE" in rule_codes


def test_rule_6_email_repeated_symbols():
    """Rule 6: Repair repeated @@ or .. in email address."""
    record = {
        "order_id": "ORD-6",
        "customer_id": "C-6",
        "customer_email": "user@@mail..com",
        "total_amount": "1000",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"A","qty":1,"unit_price":1000,"total":1000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["customer_email"] == "user@mail.com"
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "EMAIL_REPEATED_SYMBOLS" in rule_codes


def test_rule_7_date_iso_normalization():
    """Rule 7: Normalize dates and timestamps (e.g. 31/01/2025, YYYY/MM/DD HH:MM:SS) to standard ISO format."""
    record = {
        "order_id": "ORD-7",
        "customer_id": "C-7",
        "order_date": "31/01/2025",
        "total_amount": "1000",
        "items_json": '[{"sku":"A","qty":1,"unit_price":1000,"total":1000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["order_date"] == "2025-01-31"
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "DATE_ISO_NORMALIZE" in rule_codes


def test_rule_8_whitespace_and_synonyms():
    """Rule 8: Trim padding and standardize synonyms (e.g. مدفوع -> تم الدفع,   مؤكد   -> مؤكد)."""
    record = {
        "order_id": "ORD-8",
        "customer_id": "C-8",
        "status": "  مؤكد  ",
        "payment_status": "مدفوع",
        "total_amount": "1000",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"A","qty":1,"unit_price":1000,"total":1000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["status"] == "مؤكد"
    assert result.record["payment_status"] == "تم الدفع"
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "STATUS_SYNONYM_NORMALIZE" in rule_codes


def test_rule_9_order_total_recomputed():
    """Rule 9: Recompute total_amount when missing or corrupted from items and delivery cost."""
    record = {
        "order_id": "ORD-9",
        "customer_id": "C-9",
        "total_amount": "???",
        "delivery_cost": "2000",
        "order_date": "2025-01-01",
        "items_json": '[{"sku":"SKU-1","qty":2,"unit_price":15000,"total":30000}]',
    }
    result = clean_and_classify(record)
    assert result.status == "corrected"
    assert result.record["total_amount"] == 32000.0  # 30000 items + 2000 delivery
    rule_codes = [c["rule_code"] for c in result.corrections]
    assert "TOTAL_RECOMPUTED" in rule_codes
