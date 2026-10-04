# Explain Report: Before vs After Indexes

Generated automatically by `src/phase2/explain_report.py` against the live database at run time. All numbers below are real `executionStats` values from the actual data present when this report was generated - none are hardcoded.

## Indexes and why each one was chosen

| Index | Keys | Compound | Serves query | Why |
|---|---|---|---|---|
| `idx_city` | city (asc) | no | `find_orders_by_city` | بحث بالمساواة (Equality) على المدينة - فهرس مفرد كافٍ ومثالي لهذا النمط. |
| `idx_order_date` | order_date (asc) | no | `find_orders_by_date_range` | استعلام نطاقي (Range Query) على التاريخ - فهرس مرتب يسمح بـ Range Scan بدل فحص كامل المجموعة. |
| `idx_status_payment_status` | status (asc), payment_status (asc) | yes | `find_orders_by_status_and_payment` | فهرس مركّب (Compound) - الاستعلام يفلتر بحقلين معًا؛ فهرس مركّب بنفس ترتيب الحقول المستخدمة بالفلترة أكفأ بكثير من فهرسين منفصلين (Mongo تستخدم فهرسًا واحدًا فقط عادة لكل استعلام). |
| `idx_total_amount` | total_amount (desc) | no | `find_top_orders_by_amount` | الاستعلام يرتّب تنازليًا حسب القيمة ويأخذ أعلى N - فهرس مرتب يلغي الحاجة لفرز يدوي (In-Memory Sort) بالكامل. |
| `idx_customer_id` | customer_id (asc) | no | `find_orders_by_customer` | بحث بالمساواة على معرف العميل - نمط وصول متكرر جدًا (Access Pattern)، يستحق فهرسًا مخصصًا رغم تشابهه نظريًا مع idx_city. |

## Query: `find_orders_by_status_and_payment`
- Index expected to be used: `idx_status_payment_status (Compound)`
- Why this index: فهرس مركّب (Compound) - الاستعلام يفلتر بحقلين معًا؛ فهرس مركّب بنفس ترتيب الحقول المستخدمة بالفلترة أكفأ بكثير من فهرسين منفصلين (Mongo تستخدم فهرسًا واحدًا فقط عادة لكل استعلام).

| Metric | Before Index | After Index |
|---|---|---|
| Plan stages | LIMIT > COLLSCAN | LIMIT > FETCH > IXSCAN |
| Index used | none | idx_status_payment_status |
| Total docs examined | 551 | 50 |
| Total keys examined | 0 | 50 |
| Documents returned | 50 | 50 |
| Execution time (ms) | 3 | 12 |

**Impact**
- Documents examined: 551 -> 50 (about 11x fewer)
- Execution time: 3 ms -> 12 ms

## Query: `find_top_orders_by_amount`
- Index expected to be used: `idx_total_amount`
- Why this index: الاستعلام يرتّب تنازليًا حسب القيمة ويأخذ أعلى N - فهرس مرتب يلغي الحاجة لفرز يدوي (In-Memory Sort) بالكامل.

| Metric | Before Index | After Index |
|---|---|---|
| Plan stages | SORT > COLLSCAN | LIMIT > FETCH > IXSCAN |
| Index used | none | idx_total_amount |
| Total docs examined | 1817865 | 10 |
| Total keys examined | 0 | 10 |
| Documents returned | 10 | 10 |
| Execution time (ms) | 7571 | 18 |

**Impact**
- Documents examined: 1,817,865 -> 10 (about 181,786x fewer)
- Execution time: 7571 ms -> 18 ms

## Query: `find_orders_by_customer`
- Index expected to be used: `idx_customer_id`
- Why this index: بحث بالمساواة على معرف العميل - نمط وصول متكرر جدًا (Access Pattern)، يستحق فهرسًا مخصصًا رغم تشابهه نظريًا مع idx_city.

| Metric | Before Index | After Index |
|---|---|---|
| Plan stages | SORT > COLLSCAN | SORT > FETCH > IXSCAN |
| Index used | none | idx_customer_id |
| Total docs examined | 1817865 | 1 |
| Total keys examined | 0 | 1 |
| Documents returned | 1 | 1 |
| Execution time (ms) | 3348 | 63 |

**Impact**
- Documents examined: 1,817,865 -> 1 (about 1,817,865x fewer)
- Execution time: 3348 ms -> 63 ms
