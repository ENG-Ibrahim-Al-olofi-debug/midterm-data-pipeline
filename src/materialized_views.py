"""Incremental Materialized Views for Precomputed Analytics (Phase 2 - Section 3).

Materialized Views:
1. daily_sales_summary: Daily aggregated revenue and order counts.
2. top_products_summary: Product performance summary with units sold and revenue.

Features:
- Incremental Refresh Mechanism: Updates only records changed since last watermark.
- Avoids full database recalculation on each refresh.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from src.aggregations import _safe_float
from src.mongo_setup import JsonStorage, MongoStorage, get_storage

Storage = JsonStorage | MongoStorage


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MaterializedViewsManager:
    def __init__(self, storage: Storage | None = None) -> None:
        self.storage = storage or get_storage()

    def _get_watermark(self) -> dict[str, Any]:
        """Retrieve the last watermark for incremental refresh."""
        meta = self.storage.find_one("orders_raw", "__mv_watermark__") or {}
        return meta.get("metadata", {"last_processed_row": 0, "last_timestamp": "1970-01-01T00:00:00"})

    def _save_watermark(self, meta_dict: dict[str, Any]) -> None:
        self.storage.upsert("orders_raw", "__mv_watermark__", {
            "_storage_key": "__mv_watermark__",
            "metadata": meta_dict,
            "updated_at": _now(),
        })

    def get_view(self, view_name: str) -> list[dict[str, Any]]:
        """Return the precomputed records from a materialized view collection."""
        coll_name = f"mv_{view_name}" if not view_name.startswith("mv_") else view_name
        if isinstance(self.storage, MongoStorage):
            return list(self.storage.db[coll_name].find({}, {"_id": 0}))
        return list(self.storage.collections.get(coll_name, {}).values())

    def refresh(self, force_full: bool = False) -> dict[str, Any]:
        """Incrementally refresh both Materialized Views with newly arrived/modified orders."""
        start_t = time.perf_counter()
        watermark = self._get_watermark()
        last_time = watermark.get("last_timestamp", "1970-01-01T00:00:00") if not force_full else "1970-01-01T00:00:00"

        # 1. Fetch only orders updated/inserted after last_time
        all_orders = []
        if isinstance(self.storage, MongoStorage):
            all_orders = list(self.storage.db.orders_validated.find(
                {"processed_at": {"$gt": last_time}} if not force_full else {},
                {"_id": 0}
            ))
        else:
            orders_bucket = self.storage.collections.get("orders_validated", {})
            for o in orders_bucket.values():
                if force_full or str(o.get("processed_at", "")) >= last_time:
                    all_orders.append(o)

        if not all_orders and not force_full:
            # Check if views exist; if empty, do initial build
            if self.storage.count("mv_daily_sales_summary") == 0:
                return self.refresh(force_full=True)
            return {
                "status": "UP_TO_DATE",
                "message": "No new validated orders since last watermark.",
                "new_orders_processed": 0,
                "views": ["daily_sales_summary", "top_products_summary"],
                "duration_seconds": round(time.perf_counter() - start_t, 4),
            }

        # 2. Incrementally aggregate daily sales delta
        daily_delta = defaultdict(lambda: {"order_count": 0, "total_sales": 0.0})
        products_delta = defaultdict(lambda: {"units_sold": 0, "total_revenue": 0.0, "name": ""})
        max_seen_timestamp = last_time

        for o in all_orders:
            p_at = str(o.get("processed_at", ""))
            if p_at > max_seen_timestamp:
                max_seen_timestamp = p_at

            # Date delta
            raw_date = str(o.get("order_date") or "")
            date_key = raw_date[:10] if len(raw_date) >= 10 else "غير محدد"
            amount = _safe_float(o.get("total_amount"))
            daily_delta[date_key]["order_count"] += 1
            daily_delta[date_key]["total_sales"] += amount

            # Product delta
            items = o.get("items_json") or []
            if isinstance(items, str):
                try:
                    items = json.loads(items)
                except Exception:
                    items = []
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        sku = str(it.get("sku", "UNKNOWN"))
                        name = str(it.get("name", sku))
                        qty = int(_safe_float(it.get("qty", 0)))
                        tot = _safe_float(it.get("total", qty * _safe_float(it.get("unit_price", 0))))
                        products_delta[sku]["units_sold"] += qty
                        products_delta[sku]["total_revenue"] += tot
                        products_delta[sku]["name"] = name

        # 3. Apply Delta to Materialized Views (Incremental Upsert)
        for date_key, delta in daily_delta.items():
            existing = self.storage.find_one("mv_daily_sales_summary", date_key) or {}
            prev_cnt = existing.get("order_count", 0) if not force_full else 0
            prev_sales = existing.get("total_sales", 0.0) if not force_full else 0.0

            new_cnt = prev_cnt + delta["order_count"]
            new_sales = round(prev_sales + delta["total_sales"], 2)
            doc = {
                "date": date_key,
                "order_count": new_cnt,
                "total_sales": new_sales,
                "avg_order_value": round(new_sales / new_cnt, 2) if new_cnt else 0.0,
                "last_refreshed_at": _now(),
            }
            self.storage.upsert("mv_daily_sales_summary", date_key, doc)

        for sku, delta in products_delta.items():
            existing = self.storage.find_one("mv_top_products_summary", sku) or {}
            prev_qty = existing.get("units_sold", 0) if not force_full else 0
            prev_rev = existing.get("total_revenue", 0.0) if not force_full else 0.0

            new_qty = prev_qty + delta["units_sold"]
            new_rev = round(prev_rev + delta["total_revenue"], 2)
            doc = {
                "sku": sku,
                "name": delta["name"] or existing.get("name", sku),
                "units_sold": new_qty,
                "total_revenue": new_rev,
                "last_refreshed_at": _now(),
            }
            self.storage.upsert("mv_top_products_summary", sku, doc)

        # 4. Advance watermark
        self._save_watermark({
            "last_timestamp": max_seen_timestamp if max_seen_timestamp != "1970-01-01T00:00:00" else _now(),
            "last_refresh_time": _now(),
            "orders_processed": len(all_orders),
        })

        elapsed = round(time.perf_counter() - start_t, 4)
        return {
            "status": "SUCCESS",
            "mode": "FULL_BUILD" if force_full else "INCREMENTAL_DELTA_REFRESH",
            "new_orders_processed": len(all_orders),
            "affected_dates": len(daily_delta),
            "affected_products": len(products_delta),
            "watermark_advanced_to": max_seen_timestamp,
            "duration_seconds": elapsed,
            "views": ["daily_sales_summary", "top_products_summary"],
        }
