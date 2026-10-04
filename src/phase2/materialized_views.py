

import os
import sys
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING, DESCENDING

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
from src.mongo_setup import get_mongo_client
from src.phase2 import aggregations


MV_META_COLLECTION = "phase2_mv_meta"
MV_DAILY_SALES_COLLECTION = "daily_sales_summary"
MV_TOP_PRODUCTS_COLLECTION = "top_products_summary"

# بداية الزمن (Epoch): عند أول تشغيل "كل شيء" يُعتبر متأثرًا.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# حجم الدفعة عند إعادة حساب الأجزاء المتأثرة (يحدّ من حجم الاستعلام)
_CHUNK_SIZE = 500
# أقصى عدد عناصر تُعاد في الرد (حتى لا يصبح JSON ضخمًا)
_SAMPLE_SIZE = 50


# بدون حد: تقرير top_products الأصلي يحمل $limit، فنمرّر رقمًا أكبر من أي عدد منتجات واقعي
_NO_LIMIT = 2 ** 31 - 1


def _report_pipeline(name: str, params: dict | None = None, pre_match: dict | None = None, skus=None) -> list:
    """
    يبني pipeline من تقرير موجود في aggregations.AGGREGATION_REGISTRY (دون تعديل ملف
    التقارير) ثم يضيف فقط فلاتر التحديث التزايدي: فلتر على الطلبات قبل بدء التقرير
    (يستفيد من الفهارس)، وفلتر على المنتجات المتأثرة بعد $unwind.
    """
    pipeline = list(aggregations.AGGREGATION_REGISTRY[name]["builder"](params or {}))
    if skus is not None:
        sku_filter = {"items.sku": {"$in": list(skus)}}
        unwind_at = next(i for i, stage in enumerate(pipeline) if "$unwind" in stage)
        pipeline.insert(unwind_at + 1, {"$match": dict(sku_filter)})
        pre_match = {"$and": [pre_match, sku_filter]} if pre_match else sku_filter
    if pre_match:
        pipeline.insert(0, {"$match": pre_match})
    return pipeline


def _get_db():
    client = get_mongo_client()
    db = client[settings.MONGO_DB_NAME]
    return client, db


def _ensure_index(collection, keys, **kwargs):
    """ينشئ فهرسًا فقط إن لم يوجد فهرس بنفس المفاتيح (تفاديًا لتعارض الأسماء)."""
    wanted = list(keys)
    for info in collection.index_information().values():
        if list(info.get("key", [])) == wanted:
            return
    collection.create_index(wanted, **kwargs)


def _ensure_indexes(db):
    """بدون هذه الفهارس يمسح كل تحديث المجموعة كاملة بدل الأجزاء المتأثرة فقط."""
    validated = db[settings.COLLECTION_VALIDATED]
    _ensure_index(validated, [("updated_at", ASCENDING)])
    _ensure_index(validated, [("items.sku", ASCENDING)])
    _ensure_index(validated, [("order_date", ASCENDING)], name="idx_order_date")  # نفس اسم indexes.py
    _ensure_index(db[MV_TOP_PRODUCTS_COLLECTION], [("sku", ASCENDING)])
    _ensure_index(db[MV_TOP_PRODUCTS_COLLECTION], [("total_revenue", DESCENDING)])


def _get_watermark(db, view_name: str, view_collection=None, full: bool = False) -> datetime:
    """العلامة المائية؛ ترجع _EPOCH (بناء كامل) إن طُلب ذلك أو كانت الـ View فارغة."""
    if full or (view_collection is not None and view_collection.estimated_document_count() == 0):
        return _EPOCH
    doc = db[MV_META_COLLECTION].find_one({"_id": view_name})
    return doc["last_refreshed_at"] if doc else _EPOCH


def _set_watermark(db, view_name: str, when: datetime):
    db[MV_META_COLLECTION].update_one(
        {"_id": view_name},
        {"$set": {"last_refreshed_at": when}},
        upsert=True,
    )


def _changed_filter(watermark: datetime, until: datetime) -> dict:
    window = {"$lte": until}
    if watermark != _EPOCH:
        window["$gt"] = watermark
    return {"updated_at": window}


def _chunks(items, size=_CHUNK_SIZE):
    items = list(items)
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _day_range(day: str) -> dict:
    """مدى نصي يغطي كل طلبات يوم معيّن (order_date نص ISO) ويستفيد من فهرس order_date."""
    next_day = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return {"order_date": {"$gte": day, "$lt": next_day}}


def _valid_day(day) -> bool:
    try:
        datetime.strptime(day, "%Y-%m-%d")
        return True
    except (TypeError, ValueError):
        return False


def _write_day(mv_collection, row: dict, when: datetime):
    orders = row["order_count"]
    mv_collection.update_one(
        {"_id": row["period"]},
        {"$set": {
            "total_sales": row["total_sales"],
            "order_count": orders,
            "avg_order_value": round(row["total_sales"] / orders, 2) if orders else 0.0,
            "last_refreshed_at": when,
        }},
        upsert=True,
    )


def _write_product(mv_collection, row: dict, when: datetime) -> str:
    # المفتاح sku|name: نفس الـ SKU قد يظهر باسمين، فلا يجوز أن يكتب أحدهما فوق الآخر
    doc_id = f"{row.get('sku')}|{row.get('name')}"
    mv_collection.update_one(
        {"_id": doc_id},
        {"$set": {
            "sku": row.get("sku"),
            "name": row.get("name"),
            "total_quantity_sold": row["total_quantity_sold"],
            "total_revenue": row["total_revenue"],
            "times_ordered": row["times_ordered"],
            "last_refreshed_at": when,
        }},
        upsert=True,
    )
    return doc_id


def refresh_daily_sales_summary(full: bool = False) -> dict:
    """
    يحدّث daily_sales_summary تزايديًا: يحدّد الأيام المتأثرة بسجلات
    تغيّرت منذ آخر تحديث فقط، ويعيد حساب تلك الأيام تحديدًا (عبر تقرير
    sales_by_period)، دون لمس بقية الأيام.
    """
    client, db = _get_db()
    try:
        _ensure_indexes(db)
        source = db[settings.COLLECTION_VALIDATED]
        mv_collection = db[MV_DAILY_SALES_COLLECTION]
        watermark = _get_watermark(db, MV_DAILY_SALES_COLLECTION, mv_collection, full)
        run_started_at = datetime.now(timezone.utc)

        # ---- بناء كامل (أول تشغيل / View فارغة / full=True) ----
        if watermark == _EPOCH:
            pipeline = _report_pipeline("sales_by_period", {"granularity": "day"})
            rows = [r for r in source.aggregate(pipeline, allowDiskUse=True) if _valid_day(r["period"])]
            for row in rows:
                _write_day(mv_collection, row, run_started_at)
            removed = mv_collection.delete_many({"_id": {"$nin": [r["period"] for r in rows]}}).deleted_count
            _set_watermark(db, MV_DAILY_SALES_COLLECTION, run_started_at)
            return {
                "view": MV_DAILY_SALES_COLLECTION,
                "mode": "full",
                "touched_days_count": len(rows),
                "touched_days": sorted(r["period"] for r in rows)[:_SAMPLE_SIZE],
                "message": f"بناء كامل: {len(rows)} يوم (حُذف {removed} يوم قديم).",
            }

        # ---- تحديث تزايدي ----
        touched_days = [
            doc["_id"] for doc in source.aggregate([
                {"$match": _changed_filter(watermark, run_started_at)},
                {"$match": {"order_date": {"$type": "string"}}},
                {"$group": {"_id": {"$substrCP": ["$order_date", 0, 10]}}},
            ], allowDiskUse=True)
            if _valid_day(doc["_id"])
        ]

        for batch in _chunks(touched_days):
            pipeline = _report_pipeline(
                "sales_by_period", {"granularity": "day"},
                pre_match={"$or": [_day_range(day) for day in batch]},
            )
            rows = [r for r in source.aggregate(pipeline, allowDiskUse=True) if _valid_day(r["period"])]
            for row in rows:
                _write_day(mv_collection, row, run_started_at)
            vanished = set(batch) - {r["period"] for r in rows}
            if vanished:
                mv_collection.delete_many({"_id": {"$in": list(vanished)}})

        _set_watermark(db, MV_DAILY_SALES_COLLECTION, run_started_at)

        if not touched_days:
            message = "لا توجد سجلات جديدة أو معدَّلة منذ آخر تحديث - لم يُعَد حساب أي يوم."
        else:
            message = f"تم تحديث {len(touched_days)} يوم فقط (الأيام غير المتأثرة لم تُعَد حسابها)."
        return {
            "view": MV_DAILY_SALES_COLLECTION,
            "mode": "incremental",
            "touched_days_count": len(touched_days),
            "touched_days": sorted(touched_days)[:_SAMPLE_SIZE],
            "message": message,
        }
    finally:
        client.close()


def refresh_top_products_summary(full: bool = False) -> dict:
    """
    يحدّث top_products_summary تزايديًا: يحدّد المنتجات (SKU) المتأثرة
    بسجلات تغيّرت منذ آخر تحديث فقط، ويعيد حساب إجمالياتها (عبر تقرير
    top_products)، دون لمس بقية المنتجات.
    """
    client, db = _get_db()
    try:
        _ensure_indexes(db)
        source = db[settings.COLLECTION_VALIDATED]
        mv_collection = db[MV_TOP_PRODUCTS_COLLECTION]
        watermark = _get_watermark(db, MV_TOP_PRODUCTS_COLLECTION, mv_collection, full)
        run_started_at = datetime.now(timezone.utc)

        # ---- بناء كامل ----
        if watermark == _EPOCH:
            pipeline = _report_pipeline("top_products", {"limit": _NO_LIMIT})
            rows = list(source.aggregate(pipeline, allowDiskUse=True))
            ids = [_write_product(mv_collection, row, run_started_at) for row in rows]
            removed = mv_collection.delete_many({"_id": {"$nin": ids}}).deleted_count
            _set_watermark(db, MV_TOP_PRODUCTS_COLLECTION, run_started_at)
            return {
                "view": MV_TOP_PRODUCTS_COLLECTION,
                "mode": "full",
                "touched_products_count": len(rows),
                "touched_skus": [],
                "message": f"بناء كامل: {len(rows)} منتج (حُذف {removed} قديم).",
            }

        # ---- تحديث تزايدي ----
        touched_skus = [
            doc["_id"] for doc in source.aggregate([
                {"$match": _changed_filter(watermark, run_started_at)},
                {"$match": {"items": {"$type": "array"}}},
                {"$unwind": "$items"},
                {"$group": {"_id": "$items.sku"}},
            ], allowDiskUse=True)
        ]

        for batch in _chunks(touched_skus):
            pipeline = _report_pipeline("top_products", {"limit": _NO_LIMIT}, skus=batch)
            rows = list(source.aggregate(pipeline, allowDiskUse=True))
            ids = {_write_product(mv_collection, row, run_started_at) for row in rows}
            existing = {d["_id"] for d in mv_collection.find({"sku": {"$in": batch}}, {"_id": 1})}
            stale = existing - ids
            if stale:
                mv_collection.delete_many({"_id": {"$in": list(stale)}})

        _set_watermark(db, MV_TOP_PRODUCTS_COLLECTION, run_started_at)

        if not touched_skus:
            message = "لا توجد سجلات جديدة أو معدَّلة منذ آخر تحديث - لم يُعَد حساب أي منتج."
        else:
            message = f"تم تحديث {len(touched_skus)} منتج فقط (المنتجات غير المتأثرة لم تُعَد حسابها)."
        return {
            "view": MV_TOP_PRODUCTS_COLLECTION,
            "mode": "incremental",
            "touched_products_count": len(touched_skus),
            "touched_skus": sorted(str(s) for s in touched_skus)[:_SAMPLE_SIZE],
            "message": message,
        }
    finally:
        client.close()


def refresh_all_materialized_views(full: bool = False) -> dict:
    """نقطة دخول موحدة تُستخدم من الـ API (POST /refresh-mv) ومن المهام المجدولة."""
    daily_result = refresh_daily_sales_summary(full=full)
    products_result = refresh_top_products_summary(full=full)
    return {
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "full_rebuild_requested": bool(full),
        "views": [daily_result, products_result],
    }


if __name__ == "__main__":
    print("[materialized_views] تحديث العرضين الماديين...")
    summary = refresh_all_materialized_views(full="--full" in sys.argv[1:])
    for view_result in summary["views"]:
        print(f"\n=== {view_result['view']} ({view_result['mode']}) ===")
        print(f"    {view_result['message']}")