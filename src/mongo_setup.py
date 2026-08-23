

import os
import sys

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def get_mongo_client() -> MongoClient:
    
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
    
    db = get_database()

    raw_col = db[settings.COLLECTION_RAW]
    validated_col = db[settings.COLLECTION_VALIDATED]
    quarantine_col = db[settings.COLLECTION_QUARANTINE]

    
    raw_col.create_index([("id_run", ASCENDING)])

    
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