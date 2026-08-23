
import csv
import os
import sys
import time
from datetime import datetime, timezone

from pymongo.errors import BulkWriteError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from src.mongo_setup import get_database


def _build_raw_document(row: dict, row_number: int, run_context: dict) -> dict:
   
    return {
        "id_run": run_context["id_run"],
        "file_source": run_context["file_path"],
        "number_row_source": row_number,
        "at_ingested": datetime.now(timezone.utc),
        "engine_used": "python_batch",
        "record_raw": row,  
    }


def load_batches(file_path: str, run_context: dict) -> dict:
    
    db = get_database()
    raw_collection = db[settings.COLLECTION_RAW]

    start_time = time.time()
    batch = []
    batch_number = 0
    total_read = 0
    total_loaded = 0
    failed_batches = []

    with open(file_path, "r", encoding=settings.CSV_ENCODING, newline="") as infile:
        reader = csv.DictReader(infile)

        for row_number, row in enumerate(reader, start=1):
            total_read += 1
            batch.append(_build_raw_document(row, row_number, run_context))

            if len(batch) >= settings.BATCH_SIZE:
                inserted, batch_number = _flush_batch(
                    raw_collection, batch, batch_number, start_time, failed_batches
                )
                total_loaded += inserted
                batch = []

        
        if batch:
            inserted, batch_number = _flush_batch(
                raw_collection, batch, batch_number, start_time, failed_batches
            )
            total_loaded += inserted

    elapsed = time.time() - start_time
    throughput = round(total_loaded / elapsed, 2) if elapsed > 0 else 0

    summary = {
        "id_run": run_context["id_run"],
        "engine_used": "python_batch",
        "read_rows": total_read,
        "loaded_raw": total_loaded,
        "batches_total": batch_number,
        "batches_failed": len(failed_batches),
        "failed_batch_details": failed_batches,
        "seconds_elapsed": round(elapsed, 2),
        "throughput_rows_per_sec": throughput,
        "batch_size_setting": settings.BATCH_SIZE,
    }

    print("-" * 60)
    print("[batch_loader] ملخص التحميل النهائي:")
    for key, value in summary.items():
        if key != "failed_batch_details":
            print(f"    {key}: {value}")
    print("-" * 60)

    return summary


def _flush_batch(collection, batch, batch_number, start_time, failed_batches):
    
    batch_number += 1
    batch_start = time.time()

    try:
        result = collection.insert_many(batch, ordered=False)
        inserted_count = len(result.inserted_ids)
        status = "OK"
    except BulkWriteError as exc:
       
        inserted_count = exc.details.get("nInserted", 0)
        status = "PARTIAL_FAILURE"
        failed_batches.append(
            {
                "batch_number": batch_number,
                "attempted": len(batch),
                "inserted": inserted_count,
                "error": str(exc.details.get("writeErrors", exc)),
            }
        )
        print(f"[batch_loader] ⚠️ خطأ في الدفعة رقم {batch_number}: {exc}")
    except Exception as exc:  
        inserted_count = 0
        status = "FAILED"
        failed_batches.append(
            {
                "batch_number": batch_number,
                "attempted": len(batch),
                "inserted": 0,
                "error": str(exc),
            }
        )
        print(f"[batch_loader] ❌ فشلت الدفعة رقم {batch_number} بالكامل: {exc}")

    batch_elapsed = time.time() - batch_start
    batch_rate = round(inserted_count / batch_elapsed, 2) if batch_elapsed > 0 else 0

    if batch_number % settings.LOG_EVERY_N_BATCHES == 0:
        total_elapsed = time.time() - start_time
        print(
            f"[batch_loader] دفعة #{batch_number} | حالة: {status} | "
            f"سجلات: {inserted_count}/{len(batch)} | "
            f"زمن الدفعة: {batch_elapsed:.2f}s | "
            f"معدل الدفعة: {batch_rate} صف/ثانية | "
            f"الزمن الكلي حتى الآن: {total_elapsed:.2f}s"
        )

    return inserted_count, batch_number


if __name__ == "__main__":
    
    import argparse
    from src.file_router import route_file

    parser = argparse.ArgumentParser(description="اختبار مستقل لمحرك Python Batch Loader")
    parser.add_argument("--input", required=True, help="مسار ملف CSV صغير")
    args = parser.parse_args()

    ctx = route_file(args.input)
    if ctx["engine"] != "python_batch":
        print(
            f"⚠️ تحذير: الملف حجمه {ctx['file_size_mb']}MB وهو أكبر من الحد "
            f"({ctx['threshold_mb']}MB)، القرار الصحيح هو استخدام PySpark وليس هذا المحرك."
        )

    load_batches(args.input, ctx)