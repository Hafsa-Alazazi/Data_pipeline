"""
src/elt_pipeline.py
---------------------
يُنفّذ المراحل 4-6-7 من معمارية القسم 4 من الوثيقة:
    4) Quality & Transform : تطبيق quality_rules.classify_record على كل سجل خام
    5) Classification      : تصنيف كل سجل إلى Valid/Corrected/Quarantined
    6) Final Load          : كتابة Upsert إلى orders_validated و orders_quarantine
    7) Idempotency Check   : التحقق من معادلة الاتساق (القسم 6.11)

مبدأ التصميم (القسم 9): هذا الملف "يُنسّق" فقط بين القراءة، التنظيف،
والكتابة، لكن منطق التنظيف نفسه معزول بالكامل في quality_rules.py، ومنطق
الاتصال بقاعدة البيانات معزول في mongo_setup.py.

ملاحظة Streaming: نقرأ السجلات من orders_raw عبر Cursor (لا نحمّلها كلها
دفعة واحدة في الذاكرة)، ونكتب النتائج على دفعات (bulk_write) بنفس مبدأ
BATCH_SIZE المستخدم في batch_loader.py.
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone

from pymongo import UpdateOne

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from src.mongo_setup import get_database
from src.quality_rules import classify_record


def _compute_content_hash(cleaned: dict, quality_status: str, corrections: list) -> str:
    """
    يحسب بصمة (SHA-256) لمحتوى السجل "التجاري" فقط (القيم الفعلية +
    حالة الجودة + التصحيحات)، بدون أي حقول وصفية متغيرة مثل الوقت أو
    id_run. هذه البصمة هي التي تحدد إن كان السجل "تغيّر فعليًا" أم لا،
    بدل الاعتماد على وجود عملية Upsert وحدها (التي تُنفَّذ دائمًا حتى
    لو كانت القيم متطابقة).
    """
    payload = {
        "cleaned": {k: v for k, v in cleaned.items() if k != "order_id"},
        "quality_status": quality_status,
        "corrections": corrections,
    }
    serialized = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _build_validated_update(id_order: str, cleaned: dict, quality_status: str,
                             corrections: list, id_run: str) -> UpdateOne:
    """
    يبني عملية Upsert شرطية (Pipeline Update) على orders_validated:
      - إذا كانت البصمة (content_hash) الجديدة مطابقة للبصمة المخزنة
        مسبقًا، فلا نُغيّر updated_at ولا last_id_run إطلاقًا، فتُحسب
        MongoDB الوثيقة كـ "غير معدّلة" (Unchanged) بدقة.
      - إذا اختلفت البصمة (سجل جديد أو تغيّرت قيمه فعليًا)، نُحدّث كل
        الحقول بما فيها updated_at و last_id_run.

    هذا هو ما يجعل عدادات count_updated/count_unchanged في القسم 6.12
    ذات معنى حقيقي، بدل أن تكون كل عملية "update" دائمًا بسبب طابع
    زمني متغيّر لا علاقة له بالحالة التجارية الفعلية للسجل.
    """
    content_hash = _compute_content_hash(cleaned, quality_status, corrections)
    now = datetime.now(timezone.utc)

    content_fields = dict(cleaned)

    # إصلاح #1: cleaned يحتوي دائمًا items_json (السلسلة النصية الخام من
    # CSV) و items (القائمة المُحلَّلة) معًا بنفس المعلومة مكررة. نُبقي
    # items فقط في orders_validated (هي الصيغة المفيدة فعليًا للاستعلام).
    content_fields.pop("items_json", None)

    # إصلاح #2: cleaned يحتوي order_id (اسم عمود CSV الأصلي) و سنضيف
    # id_order بالأسفل (اسم Business Key الرسمي حسب القسم 6.10) بنفس
    # القيمة. نحذف order_id لتفادي وجود حقلين مكررين بنفس المعنى.
    content_fields.pop("order_id", None)

    content_fields["id_order"] = id_order
    content_fields["quality_status"] = quality_status
    content_fields["corrections"] = corrections
    content_fields["content_hash"] = content_hash

    pipeline_update = [
        {
            "$set": {
                **content_fields,
                "last_id_run": {
                    "$cond": [{"$eq": ["$content_hash", content_hash]}, "$last_id_run", id_run]
                },
                "updated_at": {
                    "$cond": [{"$eq": ["$content_hash", content_hash]}, "$updated_at", now]
                },
            }
        }
    ]

    return UpdateOne({"id_order": id_order}, pipeline_update, upsert=True)


def _build_quarantine_doc(raw_doc: dict, codes: list, corrections: list, id_run: str) -> dict:
    """
    يبني وثيقة orders_quarantine وفق القسم 6.9: يجب أن تحتوي codes_error
    وdetails_error والسجل الخام كاملاً (record_raw) لعدم فقدان أي بيانات.

    codes: قائمة أسباب (وليس رمزًا واحدًا)، لأن نفس السجل قد يُعزل لأكثر
    من سبب بنفس الوقت (مثل ERRORS_CONFLICTING_MULTIPLE، أو سجل مكرر
    id_order وله أيضًا سبب عزل أصلي آخر — انظر إصلاح #4 في run_elt).
    """
    if isinstance(codes, str):
        codes = [codes]

    record_raw = raw_doc.get("record_raw", {})
    return {
        "id_run": id_run,
        "id_order": record_raw.get("order_id") or record_raw.get("\ufefforder_id"),
        "codes_error": codes,
        "details_error": " | ".join(_describe_quarantine_code(c) for c in codes),
        "partial_corrections_attempted": corrections,
        "record_raw": record_raw,
        "number_row_source": raw_doc.get("number_row_source"),
        "quarantined_at": datetime.now(timezone.utc),
    }


_QUARANTINE_DESCRIPTIONS = {
    "ID_ORDER_MISSING": "معرف الطلب مفقود ولا يمكن استنتاجه.",
    "ID_CUSTOMER_MISSING": "معرف العميل مفقود.",
    "DATE_IMPOSSIBLE_INVALID": "تاريخ غير منطقي أو مستحيل.",
    "JSON_ITEMS_CORRUPTED": "JSON ناقص أو غير قابل للتحليل.",
    "ITEMS_EMPTY": "لا توجد عناصر للطلب.",
    "PRICE_UNKNOWN": "السعر الأصلي غير موجود أو غير قابل للاستنتاج.",
    "VALUE_NEGATIVE_AMBIGUOUS": "كمية أو مبلغ سالب لا يمكن تحديد معناه.",
    "EMAIL_UNFIXABLE": "بريد إلكتروني تالف لا يمكن إصلاحه بأمان.",
    "ID_ORDER_DUPLICATE": "تكرار يحتاج سياسة دمج أو مراجعة.",
    "ERRORS_CONFLICTING_MULTIPLE": "عدة أخطاء جوهرية تمنع التصحيح الآمن.",
}


def _describe_quarantine_code(code: str) -> str:
    return _QUARANTINE_DESCRIPTIONS.get(code, "سبب غير مصنف.")


def _get_duplicate_order_ids(raw_col, id_run: str) -> set:
    """
    يكتشف كل id_order الذي يظهر أكثر من مرة داخل نفس id_run، عبر تجميع
    (Aggregation) يُنفَّذ على خادم MongoDB مباشرة (لا نحمّل السجلات
    كاملة للذاكرة لهذا الفحص، فقط قيم المعرفات).

    السبب: نفس id_order قد يظهر أكثر من مرة في نفس الملف (بيانات قذرة
    حقيقية)، والوثيقة (القسم 6.8) تنص أن هذه الحالة "تحتاج سياسة دمج أو
    مراجعة" وليس حلاً تلقائيًا اعتباطيًا. لذلك نعزل كل نسخها بدل ترك
    آخر نسخة "تفوز" بشكل غير حتمي (يعتمد على ترتيب القراءة من Mongo
    الذي لا يُضمن ثباته بين تشغيل وآخر) — وهذا ما كان يكسر Idempotency.

    نتعامل مع احتمال وجود رمز BOM في اسم الحقل الأول (order_id) بسبب
    ملفات CSV قديمة عبر $ifNull بين الاسمين المحتملين.
    """
    pipeline = [
        {"$match": {"id_run": id_run}},
        {
            "$group": {
                "_id": {"$ifNull": ["$record_raw.order_id", "$record_raw.\ufefforder_id"]},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}, "_id": {"$ne": None}}},
    ]
    return {doc["_id"] for doc in raw_col.aggregate(pipeline) if doc["_id"] not in (None, "")}


def run_elt(id_run: str) -> dict:
    """
    الدالة الرئيسية: تقرأ كل سجلات orders_raw الخاصة بـ id_run معيّن،
    تصنّفها، وتكتب النتائج بدفعات إلى orders_validated (Upsert) و
    orders_quarantine، ثم تتحقق من اتساق العدادات (القسم 6.11).
    """
    db = get_database()
    raw_col = db[settings.COLLECTION_RAW]
    validated_col = db[settings.COLLECTION_VALIDATED]
    quarantine_col = db[settings.COLLECTION_QUARANTINE]

    start_time = time.time()

    counters = {
        "run_raw_count": 0,
        "count_valid": 0,
        "count_corrected": 0,
        "count_quarantine": 0,
        "count_inserted": 0,
        "count_updated": 0,
        "count_unchanged": 0,
    }
    counts_case_error = {}

    validated_ops_buffer = []
    quarantine_docs_buffer = []

    # فحص أولي (مرة واحدة، عبر الخادم) لكل id_order مكرر داخل هذا التشغيل
    duplicate_order_ids = _get_duplicate_order_ids(raw_col, id_run)
    if duplicate_order_ids:
        print(f"[elt_pipeline] ⚠️ تم اكتشاف {len(duplicate_order_ids)} معرف طلب مكرر داخل هذا التشغيل، سيُعزل جميعها.")

    cursor = raw_col.find({"id_run": id_run}, batch_size=settings.BATCH_SIZE)

    for raw_doc in cursor:
        counters["run_raw_count"] += 1
        result = classify_record(raw_doc.get("record_raw", {}))

        # تجاوز فوري إلى Quarantine لو كان id_order ضمن المكررات، بغض
        # النظر عن نتيجة classify_record الأصلية (حتى لو كان "صالحًا")
        record_order_id = raw_doc.get("record_raw", {}).get("order_id") or \
            raw_doc.get("record_raw", {}).get("\ufefforder_id")
        if record_order_id in duplicate_order_ids:
            counters["count_quarantine"] += 1

            # إصلاح #4: لو كان السجل أصلًا معزولاً لسبب آخر (مثلاً سعر
            # سالب)، القرار القديم كان يستبدل السبب الأصلي بـ
            # ID_ORDER_DUPLICATE ويفقده تمامًا من codes_error. الآن نجمع
            # الاثنين معًا: سبب التكرار + كل الأسباب الجوهرية الأصلية إن
            # وُجدت.
            codes = ["ID_ORDER_DUPLICATE"]
            if result["quality_status"] == "quarantined":
                codes.extend(result.get("quarantine_codes") or [result["quarantine_code"]])

            for code in codes:
                counts_case_error[code] = counts_case_error.get(code, 0) + 1

            quarantine_docs_buffer.append(
                _build_quarantine_doc(raw_doc, codes, result.get("corrections", []), id_run)
            )

        elif result["quality_status"] == "quarantined":
            counters["count_quarantine"] += 1
            codes = result.get("quarantine_codes") or [result["quarantine_code"]]
            for code in codes:
                counts_case_error[code] = counts_case_error.get(code, 0) + 1
            quarantine_docs_buffer.append(
                _build_quarantine_doc(raw_doc, codes, result["corrections"], id_run)
            )
        else:
            if result["quality_status"] == "valid":
                counters["count_valid"] += 1
            else:
                counters["count_corrected"] += 1

            cleaned = result["cleaned_record"]
            id_order = cleaned.get("order_id")
            validated_ops_buffer.append(
                _build_validated_update(
                    id_order, cleaned, result["quality_status"], result["corrections"], id_run
                )
            )

        if len(validated_ops_buffer) >= settings.BATCH_SIZE:
            _flush_validated(validated_col, validated_ops_buffer, counters)
            validated_ops_buffer = []

        if len(quarantine_docs_buffer) >= settings.BATCH_SIZE:
            quarantine_col.insert_many(quarantine_docs_buffer)
            quarantine_docs_buffer = []

    # تفريغ ما تبقى في المخازن المؤقتة
    if validated_ops_buffer:
        _flush_validated(validated_col, validated_ops_buffer, counters)
    if quarantine_docs_buffer:
        quarantine_col.insert_many(quarantine_docs_buffer)

    elapsed = time.time() - start_time

    # ---- القسم 6.11: التحقق من معادلة الاتساق ----
    expected_total = counters["count_valid"] + counters["count_corrected"] + counters["count_quarantine"]
    consistency_ok = expected_total == counters["run_raw_count"]

    summary = {
        "id_run": id_run,
        "seconds_elapsed": round(elapsed, 2),
        "counts_case_error": counts_case_error,
        "consistency_check_passed": consistency_ok,
        **counters,
    }

    print("-" * 60)
    print("[elt_pipeline] ملخص التنظيف والتصنيف:")
    for key, value in summary.items():
        print(f"    {key}: {value}")
    print(f"[elt_pipeline] معادلة الاتساق: {counters['run_raw_count']} == "
          f"{counters['count_valid']} + {counters['count_corrected']} + {counters['count_quarantine']} "
          f"→ {'✅ متحقق' if consistency_ok else '❌ فشل التحقق!'}")
    print("-" * 60)

    if not consistency_ok:
        raise AssertionError(
            f"فشل التحقق من الاتساق لـ id_run={id_run}: "
            f"raw={counters['run_raw_count']} != "
            f"valid+corrected+quarantine={expected_total}"
        )

    return summary


def _flush_validated(validated_col, ops: list, counters: dict):
    """
    ينفّذ دفعة من عمليات Upsert على orders_validated، ويحدّث عدادات
    count_inserted / count_updated / count_unchanged بدقة اعتمادًا على
    نتيجة bulk_write الفعلية من MongoDB.
    """
    result = validated_col.bulk_write(ops, ordered=False)
    inserted = result.upserted_count
    modified = result.modified_count
    matched = result.matched_count
    unchanged = matched - modified

    counters["count_inserted"] += inserted
    counters["count_updated"] += modified
    counters["count_unchanged"] += unchanged


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="اختبار مستقل لمرحلة ELT (تنظيف وتصنيف وUpsert)")
    parser.add_argument("--id-run", required=True, help="id_run الموجود مسبقًا في orders_raw")
    args = parser.parse_args()

    run_elt(args.id_run)