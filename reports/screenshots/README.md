# دليل لقطات الشاشة المطلوبة للمناقشة والتسليم (Section 11)

وفقاً لوثيقة المشروع (البند 10 و 11)، يُطلب إرفاق لقطات شاشة تثبت تشغيل المكونات عملياً:

## 1. لقطات MongoDB Compass:
1. **مجموعة `orders_raw`**:
   - إظهار أن البيانات وصلت غير منظفة (خام) وبها حقول التتبع:
     `run_id`, `source_file`, `source_row_number`, `ingested_at`, `engine_used`, `raw_record`.
2. **مجموعة `orders_validated`**:
   - إظهار الفهرس الفريد `Unique Index` على حقل `order_id`.
   - إظهار سجل مصحح بحالة `quality_status = "corrected"` ومصفوفة `corrections` (Audit Trail).
3. **مجموعة `orders_quarantine`**:
   - إظهار السجلات المعزولة مع حقول `error_codes` و `error_details` والسجل الأصلي `raw_record`.

---

## 2. لقطات Spark UI (المنفذ 4040):
عند تشغيل المسار الكبير عبر PySpark، تفتح واجهة Spark UI تلقائياً على الرابط:
`http://localhost:4040`
اللقطات المطلوبة:
1. **تبويب Jobs**: إظهار الـ Job المنفذ وحالته المكتملة `Succeeded`.
2. **تبويب Stages**: إظهار عدد الـ Stages والـ Tasks وزمن التنفيذ.
3. **تبويب Executors**: إظهار استهلاك الذاكرة والـ GC والـ Task distribution بين المعالجات.
