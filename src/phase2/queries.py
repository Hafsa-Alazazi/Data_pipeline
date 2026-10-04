

import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
from src.mongo_setup import get_mongo_client


def _get_collection():
    client = get_mongo_client()
    db = client[settings.MONGO_DB_NAME]
    return client, db[settings.COLLECTION_VALIDATED]


# --------------------------------------------------------------------------
# بناء الاستعلامات: كل دالة تعيد (filter, sort, limit) - لا تُنفَّذ هنا.
# --------------------------------------------------------------------------

def _build_find_orders_by_city(params: dict):
    city = params.get("city")
    if not city:
        raise ValueError("المعامل المطلوب مفقود: city")
    limit = int(params.get("limit", 20))
    return {"city": city}, None, limit


def _build_find_orders_by_date_range(params: dict):
    """
    ملاحظة مهمة: order_date مخزَّن في orders_validated كنص (String) بصيغة
    ISO ثابتة الطول (YYYY-MM-DDTHH:MM:SS)، وليس كنوع BSON Date حقيقي
    (راجع src/quality_rules.py: normalize_date() تنتج نصًا، لا datetime).
    لذلك نقارن القيم هنا كنصوص مباشرة (String vs String) - صيغة ISO
    ثابتة الطول تحافظ على نفس ترتيب المقارنة الزمنية الصحيح عند المقارنة
    النصية المعجمية (Lexicographic). تحويل المدخلات إلى كائن datetime هنا
    كان يسبب عدم تطابق صامت في نوع البيانات (BSON String مقابل BSON
    Date) يُرجع نتائج فارغة دائمًا دون أي خطأ ظاهر - تم اكتشاف هذا
    وإصلاحه أثناء بناء متطلبات المشروع النهائي.
    """
    start = params.get("start_date")
    end = params.get("end_date")
    if not start or not end:
        raise ValueError("المعاملات المطلوبة مفقودة: start_date, end_date")
    limit = int(params.get("limit", 50))

    # إن كان end_date تاريخًا فقط (YYYY-MM-DD) فنقصد اليوم كاملاً: بدون هذا الاستثناء
    # تستبعد المقارنة النصية "2025-01-31T10:00:00 <= 2025-01-31" كل طلبات يوم النهاية.
    end_condition = {"$lte": end}
    if len(str(end)) == 10:
        try:
            next_day = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            end_condition = {"$lt": next_day}
        except ValueError:
            pass

    return {"order_date": {"$gte": start, **end_condition}}, [("order_date", 1)], limit


def _build_find_orders_by_status_and_payment(params: dict):
    status = params.get("status")
    payment_status = params.get("payment_status")
    if not status or not payment_status:
        raise ValueError("المعاملات المطلوبة مفقودة: status, payment_status")
    limit = int(params.get("limit", 50))
    return {"status": status, "payment_status": payment_status}, None, limit


def _build_find_top_orders_by_amount(params: dict):
    limit = int(params.get("limit", 10))
    return {}, [("total_amount", -1)], limit


def _build_find_orders_by_customer(params: dict):
    customer_id = params.get("customer_id")
    if not customer_id:
        raise ValueError("المعامل المطلوب مفقود: customer_id")
    limit = int(params.get("limit", 50))
    return {"customer_id": customer_id}, [("order_date", -1)], limit


QUERY_REGISTRY = {
    "find_orders_by_city": {
        "params": {"city": "مطلوب", "limit": "اختياري (20)"},
        "description": "إيجاد الطلبات ضمن مدينة معيّنة (مثال: city=صنعاء).",
        "builder": _build_find_orders_by_city,
        "index_used": "idx_city",
    },
    "find_orders_by_date_range": {
        "params": {"start_date": "مطلوب YYYY-MM-DD", "end_date": "مطلوب YYYY-MM-DD", "limit": "اختياري (50)"},
        "description": "إيجاد الطلبات ضمن فترة زمنية (start_date, end_date بصيغة ISO).",
        "builder": _build_find_orders_by_date_range,
        "index_used": "idx_order_date",
    },
    "find_orders_by_status_and_payment": {
        "params": {"status": "مطلوب", "payment_status": "مطلوب", "limit": "اختياري (50)"},
        "description": "إيجاد الطلبات حسب حالة الطلب وحالة الدفع معًا (status, payment_status).",
        "builder": _build_find_orders_by_status_and_payment,
        "index_used": "idx_status_payment_status (Compound)",
    },
    "find_top_orders_by_amount": {
        "params": {"limit": "اختياري (10)"},
        "description": "أعلى الطلبات قيمةً (ترتيب تنازلي حسب total_amount).",
        "builder": _build_find_top_orders_by_amount,
        "index_used": "idx_total_amount",
    },
    "find_orders_by_customer": {
        "params": {"customer_id": "مطلوب", "limit": "اختياري (50)"},
        "description": "كل طلبات عميل معيّن (customer_id).",
        "builder": _build_find_orders_by_customer,
        "index_used": "idx_customer_id",
    },
}


def _serialize(doc: dict) -> dict:
    """يحوّل ObjectId وdatetime إلى نصوص قابلة لتحويل JSON مباشرة."""
    result = {}
    for key, value in doc.items():
        if key == "_id":
            result[key] = str(value)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def run_query(name: str, params: dict | None = None) -> dict:
    """ينفّذ استعلامًا مسجّلاً بالاسم ويعيد النتائج الفعلية كـ JSON-ready dict."""
    params = params or {}
    if name not in QUERY_REGISTRY:
        raise KeyError(f"استعلام غير معروف: {name}. المتاح: {list(QUERY_REGISTRY)}")

    definition = QUERY_REGISTRY[name]
    filter_, sort_, limit_ = definition["builder"](params)

    client, collection = _get_collection()
    try:
        cursor = collection.find(filter_)
        if sort_:
            cursor = cursor.sort(sort_)
        cursor = cursor.limit(limit_)
        results = [_serialize(doc) for doc in cursor]
    finally:
        client.close()

    return {
        "query_name": name,
        "description": definition["description"],
        "params_used": params,
        "result_count": len(results),
        "results": results,
    }


def explain_query(name: str, params: dict | None = None) -> dict:
    """
    ينفّذ نفس الاستعلام عبر .explain("executionStats") بدل التنفيذ العادي،
    لقياس الأداء الفعلي (totalDocsExamined، executionTimeMillis...) قبل
    وبعد إنشاء الفهارس - راجع src/phase2/explain_report.py للمقارنة الكاملة.
    """
    params = params or {}
    if name not in QUERY_REGISTRY:
        raise KeyError(f"استعلام غير معروف: {name}. المتاح: {list(QUERY_REGISTRY)}")

    definition = QUERY_REGISTRY[name]
    filter_, sort_, limit_ = definition["builder"](params)

    client, collection = _get_collection()
    try:
        cursor = collection.find(filter_)
        if sort_:
            cursor = cursor.sort(sort_)
        cursor = cursor.limit(limit_)
        raw_explain = cursor.explain()
    finally:
        client.close()

    stats = raw_explain.get("executionStats", {})
    return {
        "query_name": name,
        "index_expected": definition["index_used"],
        "filter": filter_,
        "sort": sort_,
        "execution_time_ms": stats.get("executionTimeMillis"),
        "total_docs_examined": stats.get("totalDocsExamined"),
        "total_keys_examined": stats.get("totalKeysExamined"),
        "n_returned": stats.get("nReturned"),
        "winning_plan_stage": raw_explain.get("queryPlanner", {})
            .get("winningPlan", {}).get("stage"),
        "raw_explain": raw_explain,
    }


if __name__ == "__main__":
    print("[queries] الاستعلامات المسجّلة:")
    for name, definition in QUERY_REGISTRY.items():
        print(f"  - {name}: {definition['description']}")