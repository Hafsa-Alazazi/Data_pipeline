"""
src/mongo_setup.py
--------------------
مسؤول عن الاتصال بـ MongoDB وإعداد المجموعات (Collections) والفهارس
(Indexes) المطلوبة حسب القسم 6.9 من الوثيقة:

    - orders_raw          : بدون Validator وبدون Unique Index (طبقة خام فقط)
    - orders_validated     : Unique Index على id_order (لدعم Upsert وIdempotency)
    - orders_quarantine    : بدون قيود خاصة، لكن يجب أن تحوي codes_error/details_error
"""

import os
import sys

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def get_mongo_client() -> MongoClient:
    """
    ينشئ اتصالاً بـ MongoDB ويتحقق من نجاحه فورًا (fail-fast) بدل اكتشاف
    الخطأ لاحقًا في منتصف تحميل ملايين السجلات.
    """
    client = MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except ConnectionFailure as exc:
        raise ConnectionError(
            f"تعذّر الاتصال بـ MongoDB على {settings.MONGO_URI}. "
            f"تأكد أن الخدمة تعمل. تفاصيل الخطأ: {exc}"
        ) from exc
    return client


def get_database():
    client = get_mongo_client()
    return client[settings.MONGO_DB_NAME]


def setup_collections():
    """
    يُنشئ المجموعات الثلاث (إن لم تكن موجودة) ويضبط الفهارس المطلوبة فقط:

      - orders_raw: لا نضيف أي index فريد هنا عمدًا. هذه الطبقة يجب أن
        تقبل أي سجل كما وصل، حتى لو مكرر أو ناقص، لأنها نسخة تاريخية
        قابلة للتتبع (Audit) وليست مصدر الحقيقة النهائي.

      - orders_validated: نضيف Unique Index على id_order لأنه Business
        Key الأساسي حسب القسم 6.10، وهذا الفهرس هو ما يمنع فعليًا ظهور
        سجلات مكررة عند إعادة التشغيل (Idempotency)، بالتعاون مع Upsert.

      - orders_quarantine: بدون فهرس فريد؛ يمكن لنفس id_order أن يظهر
        فيها أكثر من مرة عبر تشغيلات مختلفة قبل أن يُصحَّح لاحقًا.
    """
    db = get_database()

    raw_col = db[settings.COLLECTION_RAW]
    validated_col = db[settings.COLLECTION_VALIDATED]
    quarantine_col = db[settings.COLLECTION_QUARANTINE]

    # فهرس غير فريد على id_run لتسريع الاستعلامات بحسب كل تشغيل (اختياري، لكنه مفيد للتصحيح والمقاييس)
    raw_col.create_index([("id_run", ASCENDING)])

    # الفهرس الفريد الأساسي لضمان Idempotency في orders_validated
    validated_col.create_index([("id_order", ASCENDING)], unique=True, name="uniq_id_order")

    quarantine_col.create_index([("id_run", ASCENDING)])
    quarantine_col.create_index([("id_order", ASCENDING)])

    print("[mongo_setup] تم إعداد المجموعات والفهارس بنجاح:")
    print(f"    - {settings.COLLECTION_RAW}: index عادي على id_run (بدون unique)")
    print(f"    - {settings.COLLECTION_VALIDATED}: unique index على id_order")
    print(f"    - {settings.COLLECTION_QUARANTINE}: index عادي على id_run و id_order")

    return {
        "raw": raw_col,
        "validated": validated_col,
        "quarantine": quarantine_col,
    }


if __name__ == "__main__":
    setup_collections()