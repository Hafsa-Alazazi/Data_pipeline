

import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
from src.mongo_setup import get_mongo_client


def _get_collection():
    client = get_mongo_client()
    db = client[settings.MONGO_DB_NAME]
    return client, db[settings.COLLECTION_VALIDATED]


def _serialize(doc: dict) -> dict:
    """يحوّل ObjectId وdatetime إلى نصوص قابلة لتحويل JSON مباشرة."""
    result = {}
    for key, value in doc.items():
        if key == "_id" and not isinstance(value, (str, int, float)):
            result[key] = str(value)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


# --------------------------------------------------------------------------
# بناء الـ pipelines: كل دالة تعيد قائمة stages جاهزة لـ aggregate().
# --------------------------------------------------------------------------

def _build_sales_by_city(params: dict) -> list:
    limit = int(params.get("limit", 50))
    return [
        {"$group": {
            "_id": "$city",
            "total_sales": {"$sum": "$total_amount"},
            "order_count": {"$sum": 1},
            "avg_order_value": {"$avg": "$total_amount"},
        }},
        {"$sort": {"total_sales": -1}},
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "city": "$_id",
            "total_sales": 1,
            "order_count": 1,
            "avg_order_value": {"$round": ["$avg_order_value", 2]},
        }},
    ]


def _build_top_products(params: dict) -> list:
    limit = int(params.get("limit", 10))
    return [
        {"$unwind": "$items"},
        # يتحمّل اختلاف أسماء الحقول (qty/quantity، unit_price/price) وغياب total،
        # بنفس تسامح قواعد التنظيف في المشروع النصفي (quality_rules.py).
        {"$addFields": {
            "_qty": {"$convert": {
                "input": {"$ifNull": ["$items.qty", "$items.quantity"]},
                "to": "double", "onError": 0, "onNull": 0}},
            "_price": {"$convert": {
                "input": {"$ifNull": ["$items.unit_price", "$items.price"]},
                "to": "double", "onError": 0, "onNull": 0}},
            "_line_total": {"$convert": {
                "input": "$items.total", "to": "double", "onError": None, "onNull": None}},
        }},
        {"$group": {
            "_id": {"sku": "$items.sku", "name": "$items.name"},
            "total_quantity_sold": {"$sum": "$_qty"},
            "total_revenue": {"$sum": {"$ifNull": ["$_line_total", {"$multiply": ["$_price", "$_qty"]}]}},
            "times_ordered": {"$sum": 1},
        }},
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "sku": "$_id.sku",
            "name": "$_id.name",
            "total_quantity_sold": 1,
            "total_revenue": 1,
            "times_ordered": 1,
        }},
    ]


def _build_top_customers(params: dict) -> list:
    limit = int(params.get("limit", 10))
    return [
        {"$group": {
            "_id": "$customer_id",
            "customer_name": {"$first": "$customer_name"},
            "total_spent": {"$sum": "$total_amount"},
            "order_count": {"$sum": 1},
        }},
        {"$sort": {"total_spent": -1}},
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "customer_id": "$_id",
            "customer_name": 1,
            "total_spent": 1,
            "order_count": 1,
        }},
    ]


def _build_sales_by_period(params: dict) -> list:
    """
    ملاحظة مهمة: order_date مخزَّن في orders_validated كنص (String) بصيغة
    ISO ثابتة الطول (YYYY-MM-DDTHH:MM:SS)، وليس كنوع BSON Date حقيقي
    (راجع src/quality_rules.py: normalize_date() تنتج نصًا لا datetime).
    لذلك نستخرج الفترة عبر $substrCP على أول أحرف النص مباشرة، بدل
    $dateToString التي تتطلب حقلاً من نوع BSON Date فعلي - استخدامها هنا
    كان سيُرجع صفر نتائج دائمًا (لا تطابق صامت) لأن $match على
    {"$type": "date"} لا يطابق أي وثيقة فعليًا.
    """
    granularity = params.get("granularity", "month")  # "day" or "month"
    substr_length = 10 if granularity == "day" else 7  # "YYYY-MM-DD" أو "YYYY-MM"
    return [
        {"$addFields": {"period": {"$substrCP": ["$order_date", 0, substr_length]}}},
        {"$group": {
            "_id": "$period",
            "total_sales": {"$sum": "$total_amount"},
            "order_count": {"$sum": 1},
        }},
        {"$sort": {"_id": 1}},
        {"$project": {
            "_id": 0,
            "period": "$_id",
            "total_sales": 1,
            "order_count": 1,
        }},
    ]


def _build_orders_by_status(params: dict) -> list:
    return [
        {"$group": {
            "_id": "$status",
            "order_count": {"$sum": 1},
            "total_value": {"$sum": "$total_amount"},
        }},
        {"$sort": {"order_count": -1}},
        {"$project": {
            "_id": 0,
            "status": "$_id",
            "order_count": 1,
            "total_value": 1,
        }},
    ]


AGGREGATION_REGISTRY = {
    "sales_by_city": {
        "params": {"limit": "اختياري (50)"},
        "description": "إجمالي المبيعات وعدد الطلبات ومتوسط قيمة الطلب لكل مدينة.",
        "builder": _build_sales_by_city,
    },
    "top_products": {
        "params": {"limit": "اختياري (10)"},
        "description": "أكثر المنتجات مبيعًا من حيث الإيراد الكلي والكمية المباعة.",
        "builder": _build_top_products,
    },
    "top_customers": {
        "params": {"limit": "اختياري (10)"},
        "description": "أعلى العملاء إنفاقًا من حيث إجمالي المبلغ المدفوع وعدد الطلبات.",
        "builder": _build_top_customers,
    },
    "sales_by_period": {
        "params": {"granularity": "اختياري: month (افتراضي) أو day"},
        "description": "إجمالي المبيعات وعدد الطلبات مجمّعة حسب الفترة الزمنية (شهر أو يوم).",
        "builder": _build_sales_by_period,
    },
    "orders_by_status": {
        "params": {},
        "description": "توزيع عدد الطلبات وقيمتها الإجمالية حسب حالة الطلب.",
        "builder": _build_orders_by_status,
    },
}


def run_aggregation(name: str, params: dict | None = None) -> dict:
    """ينفّذ تجميعًا مسجّلاً بالاسم ويعيد النتائج الفعلية كـ JSON-ready dict."""
    params = params or {}
    if name not in AGGREGATION_REGISTRY:
        raise KeyError(f"تقرير غير معروف: {name}. المتاح: {list(AGGREGATION_REGISTRY)}")

    definition = AGGREGATION_REGISTRY[name]
    pipeline = definition["builder"](params)

    client, collection = _get_collection()
    try:
        results = [_serialize(doc) for doc in collection.aggregate(pipeline, allowDiskUse=True)]
    finally:
        client.close()

    return {
        "report_name": name,
        "description": definition["description"],
        "params_used": params,
        "result_count": len(results),
        "results": results,
    }


if __name__ == "__main__":
    print("[aggregations] تشغيل كل التقارير الخمسة بالإعدادات الافتراضية:\n")
    for name in AGGREGATION_REGISTRY:
        output = run_aggregation(name)
        print(f"=== {name} ({output['description']}) ===")
        print(f"عدد النتائج: {output['result_count']}")
        for row in output["results"][:5]:
            print(f"    {row}")
        if output["result_count"] > 5:
            print(f"    ... و{output['result_count'] - 5} نتيجة إضافية")
        print()