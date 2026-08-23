"""
src/metrics.py
----------------
مسؤول عن تجميع كل المقاييس المطلوبة في القسم 6.12 من الوثيقة لكل تشغيل
(id_run)، وحفظها في reports/results.json.

قرار تصميمي مهم: نحفظ results.json كـ "قائمة تشغيلات" (list) وليس كائن
واحد يُستبدل في كل مرة. السبب: القسم 6.10 يطلب صراحة إثبات Idempotency
عبر إعادة تشغيل نفس الملف أكثر من مرة، ومقارنة النتائج (تشغيل أول فيه
count_inserted > 0، وتشغيل ثانٍ لنفس البيانات فيه count_inserted = 0 و
count_unchanged = نفس الرقم). لو استبدلنا الملف في كل مرة، نفقد إمكانية
إثبات ذلك من نفس الملف الذي نسلّمه.

هذا الملف "يجمّع فقط" (aggregation)، ولا يتصل بقاعدة البيانات ولا يحتوي
منطق تنظيف — فصل الاهتمامات (Separation of Concerns) المطلوب في القسم 9.
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def build_metrics_record(router_ctx: dict, load_summary: dict, elt_summary: dict) -> dict:
    """
    يدمج مخرجات الثلاث مراحل (Router + Loader [Batch أو Spark] + ELT)
    في سجل واحد موحّد الشكل، بغض النظر عن أي محرك استُخدم.

    ملاحظة: load_summary قد يأتي من batch_loader (يحتوي batch_size_setting)
    أو من spark_loader (يحتوي partitions_input بدلاً منه)؛ لذلك نقرأ كلا
    الاسمين بأمان عبر .get() دون افتراض وجود أحدهما.
    """
    file_name = os.path.basename(router_ctx["file_path"])

    throughput = load_summary.get("throughput_rows_per_sec")
    if throughput is None and load_summary.get("seconds_elapsed"):
        loaded = load_summary.get("loaded_raw", 0)
        throughput = round(loaded / load_summary["seconds_elapsed"], 2) if load_summary["seconds_elapsed"] > 0 else 0

    record = {
        "id_run": router_ctx["id_run"],
        "recorded_at": datetime.now(timezone.utc).isoformat(),

        # --- معلومات الملف والمحرك ---
        "file_name": file_name,
        "file_size_mb": router_ctx["file_size_mb"],
        "used_engine": router_ctx["engine"],
        "engine_selection_reason": router_ctx["reason"],

        # --- مرحلة التحميل (Load) ---
        "read_rows": load_summary.get("read_rows"),
        "loaded_raw": load_summary.get("loaded_raw"),
        "load_seconds_elapsed": load_summary.get("seconds_elapsed"),
        "throughput_rows_per_sec": throughput,
        "batch_size_setting": load_summary.get("batch_size_setting"),
        "partitions_input": load_summary.get("partitions_input"),

        # --- مرحلة ELT (Quality & Classification) ---
        "elt_seconds_elapsed": elt_summary.get("seconds_elapsed"),
        "count_valid": elt_summary.get("count_valid"),
        "count_corrected": elt_summary.get("count_corrected"),
        "count_quarantine": elt_summary.get("count_quarantine"),
        "counts_case_error": elt_summary.get("counts_case_error"),
        "consistency_check_passed": elt_summary.get("consistency_check_passed"),

        # --- مرحلة Upsert / Idempotency (القسم 6.10) ---
        "count_inserted": elt_summary.get("count_inserted"),
        "count_updated": elt_summary.get("count_updated"),
        "count_unchanged": elt_summary.get("count_unchanged"),

        # --- الزمن الكلي لكامل الـ Pipeline (Load + ELT) ---
        "total_seconds_elapsed": round(
            (load_summary.get("seconds_elapsed") or 0) + (elt_summary.get("seconds_elapsed") or 0), 2
        ),
    }
    return record


def save_metrics(record: dict) -> str:
    """
    يضيف سجل التشغيل الحالي إلى نهاية قائمة التشغيلات المخزّنة في
    reports/results.json (يُنشئ الملف والمجلد إن لم يكونا موجودين).
    """
    os.makedirs(settings.REPORTS_DIR, exist_ok=True)

    existing_runs = []
    if os.path.exists(settings.RESULTS_JSON_PATH):
        try:
            with open(settings.RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
                existing_runs = json.load(f)
            if not isinstance(existing_runs, list):
                existing_runs = [existing_runs]
        except (json.JSONDecodeError, OSError):
            # ملف تالف أو فارغ: نبدأ قائمة جديدة بدل توقف البرنامج
            existing_runs = []

    existing_runs.append(record)

    with open(settings.RESULTS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(existing_runs, f, ensure_ascii=False, indent=2, default=str)

    return settings.RESULTS_JSON_PATH


def print_metrics_summary(record: dict):
    """طباعة ملخص نهائي مقروء لكل تشغيل (مطلوب صراحة: عدم بقاء البرنامج صامتًا)."""
    print("=" * 60)
    print("[metrics] ملخص المقاييس النهائي للتشغيل")
    print("=" * 60)
    for key, value in record.items():
        print(f"    {key}: {value}")
    print("=" * 60)


if __name__ == "__main__":
    # اختبار مستقل: يعرض آخر تشغيل محفوظ في results.json إن وجد
    if os.path.exists(settings.RESULTS_JSON_PATH):
        with open(settings.RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
            runs = json.load(f)
        print(f"[metrics] عدد التشغيلات المسجّلة حتى الآن: {len(runs)}")
        if runs:
            print_metrics_summary(runs[-1])
    else:
        print(f"[metrics] لا يوجد ملف نتائج بعد في {settings.RESULTS_JSON_PATH}")