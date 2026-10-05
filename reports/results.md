# Pipeline Run Results

| Metric | Value |
|---|---:|
| `run_id` | `idempotency-run-002` |
| `file_name` | `orders.csv` |
| `file_size_mb` | `0.000437` |
| `engine_used` | `python_batch` |
| `rows_read` | `3` |
| `raw_loaded` | `3` |
| `valid_count` | `1` |
| `corrected_count` | `1` |
| `quarantine_count` | `1` |
| `elapsed_seconds` | `0.0004` |
| `throughput` | `7672.63` |
| `batch_size` | `500` |
| `partitions` | `None` |
| `error_case_counts` | `multiple_conflicting_errors=1` |
| `correction_case_counts` | `phone_with_country_code=1, email_double_at=1, date_dd_mm_yyyy=1, status_extra_spaces=1, currency_arabic_name=1, arabic_digits_payment_amount=1` |
| `inserted_count` | `0` |
| `updated_count` | `0` |
| `unchanged_count` | `2` |
| `raw_consistency_check` | `True` |
| `storage_backend` | `json_fallback` |
| `route_reason` | `file_size_mb=0.0004 <= threshold_mb=200.0000` |
| `threshold_mb` | `200.0` |
