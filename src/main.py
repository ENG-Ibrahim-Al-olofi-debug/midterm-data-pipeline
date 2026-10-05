"""CLI entry point for the hybrid order ELT data pipeline & Phase 2 Analytics/API."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from config.settings import DATA_DIR, SMALL_FILE_THRESHOLD_MB
from src.aggregations import OrderAggregations
from src.analytics_queries import AnalyticsQueries
from src.elt_pipeline import run_idempotency_verification, run_pipeline
from src.incremental_loader import run_path_b_demonstration
from src.materialized_views import MaterializedViewsManager
from src.scheduled_jobs import ScheduledJobsManager


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hybrid Order Data Pipeline (Phase 1 & Phase 2) - Big Data Course Project"
    )
    parser.add_argument(
        "input",
        type=Path,
        nargs="?",
        default=DATA_DIR / "orders_small.csv",
        help="Path to input dirty orders CSV (default: data/orders_small.csv)",
    )
    parser.add_argument(
        "--threshold-mb",
        type=float,
        default=SMALL_FILE_THRESHOLD_MB,
        help=f"File size threshold in MB for router decision (default: {SMALL_FILE_THRESHOLD_MB} MB)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional custom identifier for the pipeline run",
    )
    parser.add_argument(
        "--force-engine",
        choices=("python_batch", "pyspark"),
        default=None,
        help="Force engine selection regardless of file size",
    )
    parser.add_argument(
        "--allow-pyspark-fallback",
        action="store_true",
        help="Allow local Python fallback if PySpark is not available",
    )
    parser.add_argument(
        "--test-idempotency",
        action="store_true",
        help="Run idempotency verification test (re-runs input to verify 0 duplicate records)",
    )
    parser.add_argument(
        "--test-incremental",
        action="store_true",
        help="Run Path B advanced incremental delta loading demonstration (3 experiments)",
    )
    parser.add_argument(
        "--run-queries",
        action="store_true",
        help="Execute the 5 practical analytics queries and explain('executionStats') benchmarks",
    )
    parser.add_argument(
        "--run-aggregations",
        action="store_true",
        help="Execute all 5 Aggregation Reports and display summary",
    )
    parser.add_argument(
        "--refresh-mv",
        action="store_true",
        help="Incrementally refresh the 2 Materialized Views (daily_sales_summary, top_products_summary)",
    )
    parser.add_argument(
        "--run-jobs",
        action="store_true",
        help="Trigger the 2 scheduled background jobs manually and display execution log",
    )
    parser.add_argument(
        "--serve-api",
        action="store_true",
        help="Launch the unified FastAPI server with Swagger docs at http://127.0.0.1:8000/docs",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for FastAPI server (default: 8000)",
    )
    return parser


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = build_parser()
    args = parser.parse_args()

    # Phase 2: Launch API
    if args.serve_api:
        import uvicorn
        print("=" * 60)
        print(f"[*] Starting Unified FastAPI Server on http://127.0.0.1:{args.port}")
        print(f"[*] Swagger UI Documentation: http://127.0.0.1:{args.port}/docs")
        print("=" * 60)
        uvicorn.run("src.api:app", host="127.0.0.1", port=args.port, reload=False)
        return

    # Phase 2: Run Queries & Explain
    if args.run_queries:
        aq = AnalyticsQueries()
        report = aq.run_explain_comparison()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return

    # Phase 2: Run Aggregations
    if args.run_aggregations:
        agg = OrderAggregations()
        reports = agg.get_all_reports()
        print(json.dumps(reports, ensure_ascii=False, indent=2))
        return

    # Phase 2: Refresh Materialized Views
    if args.refresh_mv:
        mvm = MaterializedViewsManager()
        res = mvm.refresh()
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return

    # Phase 2: Run Scheduled Jobs
    if args.run_jobs:
        sjm = ScheduledJobsManager()
        r1 = sjm.run_job("refresh_materialized_views")
        r2 = sjm.run_job("generate_periodic_analytics_report")
        print(json.dumps({"jobs_executed": [r1, r2]}, ensure_ascii=False, indent=2))
        return

    # Phase 1: Path B Incremental Demonstration
    if args.test_incremental:
        run_path_b_demonstration()
        return

    # Phase 1: Idempotency Verification
    if args.test_idempotency:
        run_idempotency_verification(args.input, threshold_mb=args.threshold_mb)
        return

    # Phase 1: Standard ELT Pipeline
    metrics = run_pipeline(
        input_path=args.input,
        threshold_mb=args.threshold_mb,
        run_id=args.run_id,
        force_engine=args.force_engine,
        allow_pyspark_fallback=args.allow_pyspark_fallback,
    )

    print("[*] Pipeline Execution Summary JSON:")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n[FATAL ERROR] Pipeline failed: {exc}", file=sys.stderr)
        sys.exit(1)
