"""Scheduled Background Jobs and Execution Logging (Phase 2 - Section 4).

Jobs:
1. refresh_materialized_views: Periodically triggers incremental refresh of Materialized Views.
2. generate_periodic_analytics_report: Periodically calculates key KPIs and writes an executive report.

Features:
- Runs on a periodic schedule (background thread)
- Allows on-demand manual trigger via API / CLI
- Complete execution history logging: start time, end time, duration, status, and records processed.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from config.settings import REPORTS_DIR
from src.aggregations import OrderAggregations
from src.materialized_views import MaterializedViewsManager
from src.mongo_setup import JsonStorage, MongoStorage, get_storage

Storage = JsonStorage | MongoStorage


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ScheduledJobsManager:
    def __init__(self, storage: Storage | None = None) -> None:
        self.storage = storage or get_storage()
        self.mv_manager = MaterializedViewsManager(self.storage)
        self.agg_manager = OrderAggregations(self.storage)
        self.history_file = REPORTS_DIR / "job_execution_history.json"
        self.execution_history: list[dict[str, Any]] = self._load_history()
        self._running = False
        self._thread: threading.Thread | None = None

        self.jobs_registry: dict[str, dict[str, Any]] = {
            "refresh_materialized_views": {
                "name": "refresh_materialized_views",
                "description": "Incrementally updates daily_sales_summary and top_products_summary views.",
                "interval_seconds": 60,
                "handler": self._job_refresh_materialized_views,
            },
            "generate_periodic_analytics_report": {
                "name": "generate_periodic_analytics_report",
                "description": "Calculates executive KPIs and saves a comprehensive analytics report.",
                "interval_seconds": 120,
                "handler": self._job_generate_periodic_analytics,
            },
        }

    def _load_history(self) -> list[dict[str, Any]]:
        if self.history_file.exists():
            try:
                return json.loads(self.history_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        return []

    def _save_history(self) -> None:
        self.history_file.write_text(
            json.dumps(self.execution_history[-100:], ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    # --- Job 1 Action ---

    def _job_refresh_materialized_views(self) -> dict[str, Any]:
        result = self.mv_manager.refresh()
        return {
            "views_refreshed": result.get("views", []),
            "orders_processed": result.get("new_orders_processed", 0),
            "status_detail": result.get("status"),
        }

    # --- Job 2 Action ---

    def _job_generate_periodic_analytics(self) -> dict[str, Any]:
        all_reports = self.agg_manager.get_all_reports()
        sales_city = all_reports.get("sales_by_city", [])
        top_prods = all_reports.get("top_products", [])

        total_rev = sum(c.get("total_revenue", 0) for c in sales_city)
        total_orders = sum(c.get("order_count", 0) for c in sales_city)

        report_summary = {
            "generated_at": _now(),
            "kpis": {
                "total_orders": total_orders,
                "total_revenue_yer": round(total_rev, 2),
                "avg_order_value_yer": round(total_rev / total_orders, 2) if total_orders else 0,
                "active_cities_count": len(sales_city),
                "top_selling_product": top_prods[0] if top_prods else None,
            },
            "detailed_reports": all_reports,
        }

        report_path = REPORTS_DIR / "periodic_analytics_report.json"
        report_path.write_text(json.dumps(report_summary, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "report_file": str(report_path.name),
            "total_orders_analyzed": total_orders,
            "total_revenue": total_rev,
        }

    # --- Manual & Scheduled Runner ---

    def run_job(self, job_name: str, trigger_mode: str = "MANUAL") -> dict[str, Any]:
        """Execute a specific job on-demand or on schedule and log execution metrics."""
        if job_name not in self.jobs_registry:
            raise KeyError(f"Job '{job_name}' not found. Available jobs: {list(self.jobs_registry.keys())}")

        job_info = self.jobs_registry[job_name]
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        started_at = _now()
        t0 = time.perf_counter()

        status = "SUCCESS"
        details: dict[str, Any] = {}
        error_message: str | None = None

        try:
            details = job_info["handler"]()
        except Exception as exc:
            status = "FAILED"
            error_message = str(exc)

        elapsed = round(time.perf_counter() - t0, 4)
        finished_at = _now()

        log_entry = {
            "job_id": job_id,
            "job_name": job_name,
            "trigger_mode": trigger_mode,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": elapsed,
            "details": details,
            "error": error_message,
        }

        self.execution_history.append(log_entry)
        self._save_history()

        return log_entry

    def list_jobs(self) -> list[dict[str, Any]]:
        """Return information about all available jobs and their latest execution status."""
        jobs_list = []
        for name, meta in self.jobs_registry.items():
            latest = next((h for h in reversed(self.execution_history) if h["job_name"] == name), None)
            jobs_list.append({
                "job_name": name,
                "description": meta["description"],
                "interval_seconds": meta["interval_seconds"],
                "latest_run": latest,
            })
        return jobs_list

    def get_history(self, limit: int = 20) -> list[dict[str, Any]]:
        return list(reversed(self.execution_history))[:limit]
