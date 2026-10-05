"""Five Aggregation Reports for Order Analytics (Phase 2 - Section 2).

Reports:
1. sales_by_city: Sales total, order count, and average order value by city.
2. top_products: Best-selling products by quantity and revenue (unwound from items_json).
3. top_customers: Top-spending customers by total spend and order frequency.
4. sales_by_date: Daily sales trend, order volume, and min/max order size.
5. order_status_distribution: Order count and volume breakdown by order & payment status.
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from src.mongo_setup import JsonStorage, MongoStorage, get_storage

Storage = JsonStorage | MongoStorage


ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _safe_float(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    import re
    s = str(val).translate(ARABIC_DIGITS).replace("٫", ".").replace(",", "").strip()
    cleaned = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(cleaned) if cleaned and cleaned != "-" else 0.0
    except Exception:
        return 0.0


class OrderAggregations:
    def __init__(self, storage: Storage | None = None) -> None:
        self.storage = storage or get_storage()

    def _get_validated_records(self) -> list[dict[str, Any]]:
        if isinstance(self.storage, MongoStorage):
            return list(self.storage.db.orders_validated.find({}, {"_id": 0}))
        return list(self.storage.collections.get("orders_validated", {}).values())

    # --- Report 1: Sales by City ---

    def report_sales_by_city(self) -> list[dict[str, Any]]:
        """Report 1: Aggregated revenue, order count, and average order value grouped by city."""
        if isinstance(self.storage, MongoStorage):
            pipeline = [
                {
                    "$group": {
                        "_id": {"$ifNull": ["$city", "غير محدد"]},
                        "order_count": {"$sum": 1},
                        "total_revenue": {"$sum": "$total_amount"},
                        "avg_order_value": {"$avg": "$total_amount"},
                    }
                },
                {"$sort": {"total_revenue": -1}},
                {
                    "$project": {
                        "_id": 0,
                        "city": "$_id",
                        "order_count": 1,
                        "total_revenue": {"$round": ["$total_revenue", 2]},
                        "avg_order_value": {"$round": ["$avg_order_value", 2]},
                    }
                }
            ]
            return list(self.storage.db.orders_validated.aggregate(pipeline))

        # Fallback implementation
        by_city = defaultdict(lambda: {"order_count": 0, "total_revenue": 0.0})
        for o in self._get_validated_records():
            city = o.get("city") or "غير محدد"
            amount = _safe_float(o.get("total_amount"))
            by_city[city]["order_count"] += 1
            by_city[city]["total_revenue"] += amount

        results = []
        for city, data in by_city.items():
            cnt = data["order_count"]
            tot = data["total_revenue"]
            results.append({
                "city": city,
                "order_count": cnt,
                "total_revenue": round(tot, 2),
                "avg_order_value": round(tot / cnt, 2) if cnt else 0,
            })
        results.sort(key=lambda x: x["total_revenue"], reverse=True)
        return results

    # --- Report 2: Top Products ---

    def report_top_products(self, limit: int = 10) -> list[dict[str, Any]]:
        """Report 2: Best-selling products by quantity and total revenue."""
        by_product = defaultdict(lambda: {"units_sold": 0, "total_revenue": 0.0, "orders_count": 0, "name": ""})
        for o in self._get_validated_records():
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
                        qty = _safe_float(it.get("qty", 0))
                        tot = _safe_float(it.get("total", qty * _safe_float(it.get("unit_price", 0))))
                        by_product[sku]["units_sold"] += int(qty)
                        by_product[sku]["total_revenue"] += tot
                        by_product[sku]["orders_count"] += 1
                        by_product[sku]["name"] = name

        results = []
        for sku, data in by_product.items():
            results.append({
                "sku": sku,
                "name": data["name"] or sku,
                "units_sold": data["units_sold"],
                "total_revenue": round(data["total_revenue"], 2),
                "orders_count": data["orders_count"],
            })
        results.sort(key=lambda x: x["total_revenue"], reverse=True)
        return results[:limit]

    # --- Report 3: Top Customers ---

    def report_top_customers(self, limit: int = 10) -> list[dict[str, Any]]:
        """Report 3: Highest-value customers ranked by total spend."""
        if isinstance(self.storage, MongoStorage):
            pipeline = [
                {
                    "$group": {
                        "_id": "$customer_id",
                        "customer_name": {"$first": "$customer_name"},
                        "order_count": {"$sum": 1},
                        "total_spend": {"$sum": "$total_amount"},
                        "avg_spend": {"$avg": "$total_amount"},
                    }
                },
                {"$sort": {"total_spend": -1}},
                {"$limit": limit},
                {
                    "$project": {
                        "_id": 0,
                        "customer_id": "$_id",
                        "customer_name": {"$ifNull": ["$customer_name", "غير معروف"]},
                        "order_count": 1,
                        "total_spend": {"$round": ["$total_spend", 2]},
                        "avg_spend": {"$round": ["$avg_spend", 2]},
                    }
                }
            ]
            return list(self.storage.db.orders_validated.aggregate(pipeline))

        by_cust = defaultdict(lambda: {"order_count": 0, "total_spend": 0.0, "name": ""})
        for o in self._get_validated_records():
            cid = str(o.get("customer_id") or "UNKNOWN")
            amount = _safe_float(o.get("total_amount"))
            by_cust[cid]["order_count"] += 1
            by_cust[cid]["total_spend"] += amount
            if o.get("customer_name"):
                by_cust[cid]["name"] = o["customer_name"]

        results = []
        for cid, data in by_cust.items():
            cnt = data["order_count"]
            tot = data["total_spend"]
            results.append({
                "customer_id": cid,
                "customer_name": data["name"] or "غير معروف",
                "order_count": cnt,
                "total_spend": round(tot, 2),
                "avg_spend": round(tot / cnt, 2) if cnt else 0,
            })
        results.sort(key=lambda x: x["total_spend"], reverse=True)
        return results[:limit]

    # --- Report 4: Sales by Date ---

    def report_sales_by_date(self) -> list[dict[str, Any]]:
        """Report 4: Daily sales distribution, order count, and min/max order size."""
        by_date = defaultdict(lambda: {"order_count": 0, "total_sales": 0.0, "amounts": []})
        for o in self._get_validated_records():
            raw_date = str(o.get("order_date") or "")
            date_key = raw_date[:10] if len(raw_date) >= 10 else "غير محدد"
            amount = _safe_float(o.get("total_amount"))
            by_date[date_key]["order_count"] += 1
            by_date[date_key]["total_sales"] += amount
            by_date[date_key]["amounts"].append(amount)

        results = []
        for d, data in by_date.items():
            amounts = data["amounts"] or [0]
            results.append({
                "date": d,
                "order_count": data["order_count"],
                "total_sales": round(data["total_sales"], 2),
                "min_order_amount": round(min(amounts), 2),
                "max_order_amount": round(max(amounts), 2),
                "avg_order_amount": round(data["total_sales"] / len(amounts), 2),
            })
        results.sort(key=lambda x: x["date"])
        return results

    # --- Report 5: Order Status Distribution ---

    def report_order_status_distribution(self) -> list[dict[str, Any]]:
        """Report 5: Cross-tabulation of orders by fulfillment status and payment status."""
        by_status = defaultdict(lambda: {"order_count": 0, "total_value": 0.0})
        total_orders = 0

        for o in self._get_validated_records():
            status = o.get("status") or "غير محدد"
            payment_status = o.get("payment_status") or "غير محدد"
            key = f"{status} | {payment_status}"
            amount = float(o.get("total_amount") or 0)
            by_status[key]["order_count"] += 1
            by_status[key]["total_value"] += amount
            total_orders += 1

        results = []
        for key, data in by_status.items():
            st, pst = key.split(" | ")
            pct = round((data["order_count"] / max(1, total_orders)) * 100, 2)
            results.append({
                "order_status": st,
                "payment_status": pst,
                "order_count": data["order_count"],
                "total_value": round(data["total_value"], 2),
                "percentage_of_total": f"{pct}%",
            })
        results.sort(key=lambda x: x["order_count"], reverse=True)
        return results

    def get_all_reports(self) -> dict[str, Any]:
        """Execute and return all 5 aggregation reports."""
        return {
            "sales_by_city": self.report_sales_by_city(),
            "top_products": self.report_top_products(),
            "top_customers": self.report_top_customers(),
            "sales_by_date": self.report_sales_by_date(),
            "order_status_distribution": self.report_order_status_distribution(),
        }
