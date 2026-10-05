"""Unified FastAPI Application for Automated Evaluation and Interactive Testing (Phase 2 - Section 5).

Endpoints:
- GET  /health
- POST /ingest
- POST /indexes
- GET  /queries
- GET  /queries/{name}
- GET  /aggregations
- GET  /aggregations/{name}
- POST /refresh-mv
- GET  /jobs
- POST /jobs/{name}/run
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config.settings import DATA_DIR, SMALL_FILE_THRESHOLD_MB
from src.aggregations import OrderAggregations
from src.analytics_queries import INDEX_DEFINITIONS, AnalyticsQueries
from src.elt_pipeline import run_pipeline
from src.materialized_views import MaterializedViewsManager
from src.mongo_setup import get_storage
from src.scheduled_jobs import ScheduledJobsManager

app = FastAPI(
    title="Hybrid Data Pipeline & Analytics API",
    description="Unified evaluation and test interface for Big Data Course Project (Phase 1 & Phase 2)",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class IngestRequest(BaseModel):
    file_path: Optional[str] = "data/orders_small.csv"
    threshold_mb: Optional[float] = SMALL_FILE_THRESHOLD_MB
    force_engine: Optional[str] = None
    run_id: Optional[str] = None


# --- 1. Health Endpoint ---

@app.get("/health", tags=["System"])
def get_health() -> dict[str, Any]:
    storage = get_storage()
    backend = storage.backend
    storage.close()
    return {
        "status": "UP",
        "service": "hybrid-order-data-pipeline",
        "storage_backend": backend,
        "default_threshold_mb": SMALL_FILE_THRESHOLD_MB,
        "python_version": sys.version.split()[0],
    }


# --- 2. Ingest Endpoint ---

@app.post("/ingest", tags=["Pipeline Execution"])
def trigger_ingest(request: IngestRequest) -> dict[str, Any]:
    target_path = Path(request.file_path) if request.file_path else DATA_DIR / "orders_small.csv"
    if not target_path.is_absolute():
        target_path = (Path(__file__).resolve().parents[1] / target_path).resolve()

    if not target_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {target_path}")

    try:
        metrics = run_pipeline(
            input_path=target_path,
            threshold_mb=request.threshold_mb or SMALL_FILE_THRESHOLD_MB,
            run_id=request.run_id,
            force_engine=request.force_engine,
        )
        return {"status": "SUCCESS", "metrics": metrics}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}")


# --- 3. Indexes & Explain Benchmark Endpoint ---

@app.post("/indexes", tags=["Indexes & Execution Plan"])
def create_indexes_and_benchmark() -> dict[str, Any]:
    aq = AnalyticsQueries()
    try:
        benchmark_report = aq.run_explain_comparison()
        return {
            "status": "SUCCESS",
            "message": "Indexes created and explain('executionStats') benchmarks completed.",
            "benchmark_report": benchmark_report,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Index benchmarking failed: {exc}")


# --- 4. Queries Endpoints ---

@app.get("/queries", tags=["Practical Queries"])
def list_available_queries() -> dict[str, Any]:
    return {
        "available_queries": [
            {
                "name": "customer_orders",
                "description": "Find orders placed by a specific customer, sorted by date desc.",
                "supported_params": {"customer_id": "string (default: 'عميل-0')", "limit": "int"},
                "served_by_index": "idx_customer_id",
            },
            {
                "name": "city_status_orders",
                "description": "Filter orders by city and order status.",
                "supported_params": {"city": "string (default: 'تعز')", "status": "string (default: 'مؤكد')", "limit": "int"},
                "served_by_index": "idx_city_status (Compound)",
            },
            {
                "name": "high_value_orders",
                "description": "Find VIP orders where total_amount >= min_amount, sorted desc.",
                "supported_params": {"min_amount": "float (default: 300000.0)", "limit": "int"},
                "served_by_index": "idx_amount_date (Compound)",
            },
            {
                "name": "date_range_orders",
                "description": "Find orders placed within a date window.",
                "supported_params": {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD", "limit": "int"},
                "served_by_index": "idx_order_date",
            },
            {
                "name": "payment_reconciliation",
                "description": "Reconcile payment transactions by payment method and status.",
                "supported_params": {"payment_method": "string", "payment_status": "string", "limit": "int"},
                "served_by_index": "idx_payment (Compound)",
            },
        ],
        "index_definitions": INDEX_DEFINITIONS,
    }


@app.get("/queries/{name}", tags=["Practical Queries"])
def execute_query(
    name: str,
    customer_id: str = "عميل-0",
    city: str = "تعز",
    status: str = "مؤكد",
    min_amount: float = 300000.0,
    start_date: str = "2025-01-01",
    end_date: str = "2025-02-28",
    payment_method: str = "محفظة إلكترونية",
    payment_status: str = "تم الدفع",
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    aq = AnalyticsQueries()
    if name == "customer_orders":
        data = aq.query_customer_orders(customer_id=customer_id, limit=limit)
    elif name == "city_status_orders":
        data = aq.query_city_status_orders(city=city, status=status, limit=limit)
    elif name == "high_value_orders":
        data = aq.query_high_value_orders(min_amount=min_amount, limit=limit)
    elif name == "date_range_orders":
        data = aq.query_date_range_orders(start_date=start_date, end_date=end_date, limit=limit)
    elif name == "payment_reconciliation":
        data = aq.query_payment_reconciliation(payment_method=payment_method, payment_status=payment_status, limit=limit)
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Query '{name}' not found. Check GET /queries for available queries.",
        )

    return {
        "query_name": name,
        "results_count": len(data),
        "results": data,
    }


# --- 5. Aggregations Endpoints ---

@app.get("/aggregations", tags=["Aggregation Reports"])
def list_available_aggregations() -> dict[str, Any]:
    return {
        "available_aggregations": [
            {
                "name": "sales_by_city",
                "description": "Total sales volume, order count, and average order value grouped by city.",
            },
            {
                "name": "top_products",
                "description": "Best-selling products unwound from items_json ranked by units sold and revenue.",
            },
            {
                "name": "top_customers",
                "description": "Highest-spending customer accounts ranked by total spending.",
            },
            {
                "name": "sales_by_date",
                "description": "Daily sales distribution, order count, and min/max order metrics.",
            },
            {
                "name": "order_status_distribution",
                "description": "Cross-tabulation of orders by fulfillment status and payment status.",
            },
        ]
    }


@app.get("/aggregations/{name}", tags=["Aggregation Reports"])
def execute_aggregation(name: str, limit: int = 10) -> dict[str, Any]:
    agg = OrderAggregations()
    if name == "sales_by_city":
        data = agg.report_sales_by_city()
    elif name == "top_products":
        data = agg.report_top_products(limit=limit)
    elif name == "top_customers":
        data = agg.report_top_customers(limit=limit)
    elif name == "sales_by_date":
        data = agg.report_sales_by_date()
    elif name == "order_status_distribution":
        data = agg.report_order_status_distribution()
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Aggregation '{name}' not found. Check GET /aggregations for list.",
        )

    return {
        "report_name": name,
        "records_count": len(data),
        "data": data,
    }


# --- 6. Materialized Views Endpoint ---

@app.post("/refresh-mv", tags=["Materialized Views"])
def refresh_materialized_views(force_full: bool = False) -> dict[str, Any]:
    mvm = MaterializedViewsManager()
    try:
        result = mvm.refresh(force_full=force_full)
        return {
            "status": "SUCCESS",
            "materialized_views_refresh": result,
            "views": {
                "daily_sales_summary_count": len(mvm.get_view("daily_sales_summary")),
                "top_products_summary_count": len(mvm.get_view("top_products_summary")),
            },
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Materialized View refresh failed: {exc}")


# --- 7. Scheduled Jobs Endpoints ---

@app.get("/jobs", tags=["Scheduled Jobs"])
def list_jobs() -> dict[str, Any]:
    manager = ScheduledJobsManager()
    return {
        "available_jobs": manager.list_jobs(),
        "recent_history": manager.get_history(limit=10),
    }


@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def trigger_job_manually(name: str) -> dict[str, Any]:
    manager = ScheduledJobsManager()
    try:
        result = manager.run_job(name, trigger_mode="MANUAL_API_TRIGGER")
        return {
            "status": "SUCCESS",
            "execution_log": result,
        }
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{name}' not found. Available jobs: {list(manager.jobs_registry.keys())}",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Job execution failed: {exc}")
