"""Unit and Integration tests for Big Data Final Project (Phase 2 Additions).

Covers:
1. Five practical analytics queries
2. Indexes & explain execution plan benchmarking
3. Five aggregation reports
4. Two incremental materialized views
5. Two scheduled jobs with execution logging
6. Unified FastAPI endpoints
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.aggregations import OrderAggregations
from src.analytics_queries import AnalyticsQueries
from src.api import app
from src.materialized_views import MaterializedViewsManager
from src.scheduled_jobs import ScheduledJobsManager


@pytest.fixture
def api_client():
    return TestClient(app)


# --- 1. Queries & Index Benchmarking Tests ---

def test_queries_and_explain_benchmarks():
    aq = AnalyticsQueries()
    report = aq.run_explain_comparison()
    assert report["queries_benchmarked"] == 3
    assert len(report["results"]) == 3
    for r in report["results"]:
        assert r["status"] == "OPTIMIZED"
        assert "%" in r["docs_scanned_reduction"]

    # Test practical queries
    cust_orders = aq.query_customer_orders(limit=10)
    assert isinstance(cust_orders, list)

    city_orders = aq.query_city_status_orders(city="تعز", status="مؤكد", limit=10)
    assert isinstance(city_orders, list)

    high_val = aq.query_high_value_orders(min_amount=100000.0, limit=10)
    assert isinstance(high_val, list)

    date_orders = aq.query_date_range_orders("2025-01-01", "2025-03-31", limit=10)
    assert isinstance(date_orders, list)

    reconcile = aq.query_payment_reconciliation("محفظة إلكترونية", "تم الدفع", limit=10)
    assert isinstance(reconcile, list)


# --- 2. Five Aggregations Tests ---

def test_five_aggregation_reports():
    agg = OrderAggregations()
    reports = agg.get_all_reports()

    assert "sales_by_city" in reports
    assert "top_products" in reports
    assert "top_customers" in reports
    assert "sales_by_date" in reports
    assert "order_status_distribution" in reports

    # Check sales by city structure
    sales_city = reports["sales_by_city"]
    if sales_city:
        assert "city" in sales_city[0]
        assert "total_revenue" in sales_city[0]
        assert "order_count" in sales_city[0]

    # Check top products structure
    top_prods = reports["top_products"]
    if top_prods:
        assert "sku" in top_prods[0]
        assert "units_sold" in top_prods[0]
        assert "total_revenue" in top_prods[0]


# --- 3. Materialized Views Tests ---

def test_materialized_views_incremental_refresh():
    mvm = MaterializedViewsManager()
    # Initial build
    res_full = mvm.refresh(force_full=True)
    assert res_full["status"] == "SUCCESS"
    assert "daily_sales_summary" in res_full["views"]
    assert "top_products_summary" in res_full["views"]

    daily_view = mvm.get_view("daily_sales_summary")
    prods_view = mvm.get_view("top_products_summary")
    assert isinstance(daily_view, list)
    assert isinstance(prods_view, list)

    # Incremental refresh
    res_inc = mvm.refresh(force_full=False)
    assert res_inc["status"] in ("SUCCESS", "UP_TO_DATE")


# --- 4. Scheduled Jobs Tests ---

def test_scheduled_jobs_execution_and_logging():
    mgr = ScheduledJobsManager()
    jobs = mgr.list_jobs()
    assert len(jobs) == 2

    # Run Job 1 manually
    r1 = mgr.run_job("refresh_materialized_views")
    assert r1["status"] == "SUCCESS"
    assert r1["job_name"] == "refresh_materialized_views"

    # Run Job 2 manually
    r2 = mgr.run_job("generate_periodic_analytics_report")
    assert r2["status"] == "SUCCESS"
    assert r2["job_name"] == "generate_periodic_analytics_report"

    # Verify history
    history = mgr.get_history()
    assert len(history) >= 2


# --- 5. FastAPI Unified Endpoints Tests ---

def test_fastapi_endpoints(api_client):
    # Health
    r = api_client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "UP"

    # Queries
    r = api_client.get("/queries")
    assert r.status_code == 200
    assert len(r.json()["available_queries"]) == 5

    r = api_client.get("/queries/city_status_orders?city=تعز&status=مؤكد")
    assert r.status_code == 200
    assert "results" in r.json()

    # Indexes
    r = api_client.post("/indexes")
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"

    # Aggregations
    r = api_client.get("/aggregations")
    assert r.status_code == 200
    assert len(r.json()["available_aggregations"]) == 5

    r = api_client.get("/aggregations/sales_by_city")
    assert r.status_code == 200
    assert "data" in r.json()

    # Materialized Views
    r = api_client.post("/refresh-mv")
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"

    # Jobs
    r = api_client.get("/jobs")
    assert r.status_code == 200

    r = api_client.post("/jobs/refresh_materialized_views/run")
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"
