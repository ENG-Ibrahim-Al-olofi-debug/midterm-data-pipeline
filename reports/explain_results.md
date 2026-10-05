# تقرير فحص خطط التنفيذ والفهارس (Explain & Indexes Execution Report)

تم تنفيذ `explain('executionStats')` لثلاثة استعلامات رئيسية قبل وبعد إنشاء الفهارس:

| الاستعلام | الفهرس المطبق | نوع الفهرس | المرحلة قبل | المرحلة بعد | فحص المستندات (قبل $\rightarrow$ بعد) | نسبة التحسين |
|---|---|---|---|---|---|---|
| `customer_order_history` | `idx_customer_id` | Single Field | `COLLSCAN (Collection Scan)` | `FETCH -> IXSCAN (Index Scan)` | 17000 $\rightarrow$ 0 | **100.0%** |
| `city_status_orders` | `idx_city_status` | Compound Index | `COLLSCAN (Collection Scan)` | `FETCH -> IXSCAN (Index Scan)` | 17000 $\rightarrow$ 410 | **97.6%** |
| `high_value_orders` | `idx_amount_date` | Compound Index | `COLLSCAN (Collection Scan)` | `FETCH -> IXSCAN (Index Scan)` | 17000 $\rightarrow$ 5676 | **66.6%** |

## أسباب اختيار الفهارس وأثرها التقني:

- **idx_customer_id (Single Field)**: Optimizes high-frequency customer portal lookups and historical purchase retrieval by customer_id.
- **idx_city_status (Compound Index)**: Compound index supporting regional logistics filtering: accelerates city-level fulfillment tracking by order status without in-memory filtering.
- **idx_amount_date (Compound Index)**: Compound index covering VIP revenue queries: enables fast range filtering on total_amount and sort on order_date without in-memory sort.
