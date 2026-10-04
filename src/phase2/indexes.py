

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
from src.mongo_setup import get_mongo_client



INDEX_DEFINITIONS = [
    {
        "name": "idx_city",
        "keys": [("city", 1)],
        "supports_query": "find_orders_by_city",
        "reason": "بحث بالمساواة (Equality) على المدينة - فهرس مفرد كافٍ ومثالي لهذا النمط.",
    },
    {
        "name": "idx_order_date",
        "keys": [("order_date", 1)],
        "supports_query": "find_orders_by_date_range",
        "reason": "استعلام نطاقي (Range Query) على التاريخ - فهرس مرتب يسمح بـ Range Scan بدل فحص كامل المجموعة.",
    },
    {
        "name": "idx_status_payment_status",
        "keys": [("status", 1), ("payment_status", 1)],
        "supports_query": "find_orders_by_status_and_payment",
        "reason": (
            "فهرس مركّب (Compound) - الاستعلام يفلتر بحقلين معًا؛ فهرس "
            "مركّب بنفس ترتيب الحقول المستخدمة بالفلترة أكفأ بكثير من "
            "فهرسين منفصلين (Mongo تستخدم فهرسًا واحدًا فقط عادة لكل استعلام)."
        ),
    },
    {
        "name": "idx_total_amount",
        "keys": [("total_amount", -1)],
        "supports_query": "find_top_orders_by_amount",
        "reason": "الاستعلام يرتّب تنازليًا حسب القيمة ويأخذ أعلى N - فهرس مرتب يلغي الحاجة لفرز يدوي (In-Memory Sort) بالكامل.",
    },
    {
        "name": "idx_customer_id",
        "keys": [("customer_id", 1)],
        "supports_query": "find_orders_by_customer",
        "reason": "بحث بالمساواة على معرف العميل - نمط وصول متكرر جدًا (Access Pattern)، يستحق فهرسًا مخصصًا رغم تشابهه نظريًا مع idx_city.",
    },
]


def create_phase2_indexes() -> dict:
    """
    ينشئ كل الفهارس المعرّفة في INDEX_DEFINITIONS داخل orders_validated.
    create_index عملية آمنة للتكرار (Idempotent) - استدعاؤها عدة مرات لا
    يكسر شيئًا ولا يعيد إنشاء فهرس موجود بنفس المواصفات.

    يعيد ملخصًا بأسماء الفهارس التي أُنشئت فعليًا (أو كانت موجودة أصلاً)،
    مبنيًا من استجابة MongoDB الفعلية - وليس قائمة ثابتة بالكود.
    """
    client = get_mongo_client()
    try:
        db = client[settings.MONGO_DB_NAME]
        collection = db[settings.COLLECTION_VALIDATED]

        # فهارس موجودة فعليًا مفهرسة بالحقول (وليس بالاسم): إن وُجد فهرس بنفس الحقول
        # باسم مختلف (مثلاً أنشأه تحديث الـ Views تلقائيًا) فإعادة إنشائه باسم آخر
        # تُسبب IndexOptionsConflict في MongoDB، لذلك نعيد استخدامه بدل إنشاء نسخة.
        existing_by_keys = {
            tuple(info["key"]): name
            for name, info in collection.index_information().items()
        }

        created = []
        for index_def in INDEX_DEFINITIONS:
            existing_name = existing_by_keys.get(tuple(index_def["keys"]))
            if existing_name is not None:
                index_name = existing_name
            else:
                index_name = collection.create_index(
                    index_def["keys"],
                    name=index_def["name"],
                )
            created.append({
                "name": index_name,
                "keys": index_def["keys"],
                "is_compound": len(index_def["keys"]) > 1,
                "supports_query": index_def["supports_query"],
                "reason": index_def["reason"],
            })
    finally:
        client.close()

    return {
        "collection": settings.COLLECTION_VALIDATED,
        "indexes_created_or_existing": created,
        "total_indexes_requested": len(INDEX_DEFINITIONS),
        "compound_indexes_count": sum(1 for i in created if i["is_compound"]),
    }


def list_existing_indexes() -> list:
    """
    يعيد قائمة الفهارس الموجودة فعليًا حاليًا في orders_validated كما
    ترجعها MongoDB نفسها (getIndexes) - للتحقق المباشر، وليس افتراضًا
    نظريًا بأنها موجودة.
    """
    client = get_mongo_client()
    try:
        db = client[settings.MONGO_DB_NAME]
        collection = db[settings.COLLECTION_VALIDATED]
        return [dict(index) for index in collection.list_indexes()]
    finally:
        client.close()


if __name__ == "__main__":
    print("[indexes] إنشاء فهارس المشروع النهائي على orders_validated...")
    summary = create_phase2_indexes()
    for idx in summary["indexes_created_or_existing"]:
        compound_tag = " (Compound)" if idx["is_compound"] else ""
        print(f"  ✅ {idx['name']}{compound_tag}: {idx['keys']}")
        print(f"     يخدم: {idx['supports_query']}")
        print(f"     السبب: {idx['reason']}")
    print(f"\n[indexes] الإجمالي: {summary['total_indexes_requested']} فهرس "
          f"({summary['compound_indexes_count']} مركّب).")