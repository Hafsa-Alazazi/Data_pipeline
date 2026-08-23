"""
src/file_router.py
--------------------
نقطة القرار الوحيدة في المشروع لاختيار محرك المعالجة (Python Batch أو PySpark)
بناءً على حجم الملف مقارنة بـ SMALL_FILE_THRESHOLD_MB.

هذا الملف لا يحتوي منطق معالجة فعلي؛ فقط "يقرر" ثم يستدعي المحرك المناسب.
منطق المعالجة الحقيقي موجود في batch_loader.py و spark_loader.py بشكل منفصل
(فصل الاهتمامات - Separation of Concerns، مطلوب في القسم 9).
"""

import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def generate_run_id() -> str:
    """
    يولّد معرف فريد لكل عملية تشغيل (id_run)، يُستخدم لربط كل السجلات
    (raw / validated / quarantine) بنفس عملية التشغيل التي أنتجتها،
    ولتطبيق معادلة الاتساق في القسم 6.11.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    return f"run_{timestamp}_{short_uuid}"


def get_file_size_mb(file_path: str) -> float:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 * 1024)


def choose_engine(file_size_mb: float) -> str:
    """
    منطق القرار المطلوب حرفيًا في القسم 6.2 من الوثيقة:

        if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
            engine = "python_batch"
        else:
            engine = "pyspark"
    """
    if file_size_mb <= settings.SMALL_FILE_THRESHOLD_MB:
        return "python_batch"
    return "pyspark"


def route_file(file_path: str) -> dict:
    """
    الدالة الرئيسية للـ Router:
      1. يفحص حجم الملف.
      2. يقرر المحرك.
      3. يطبع القرار وسببه (إلزامي حسب الوثيقة).
      4. يولّد id_run لهذه العملية.
      5. يعيد كل هذه المعلومات كـ "سياق تشغيل" (run_context) تُمرَّر
         للمراحل التالية من الـ Pipeline.
    """
    file_size_mb = get_file_size_mb(file_path)
    engine = choose_engine(file_size_mb)
    id_run = generate_run_id()

    reason = (
        f"حجم الملف ({file_size_mb:.2f} MB) "
        f"{'أقل من أو يساوي' if engine == 'python_batch' else 'أكبر من'} "
        f"الحد الفاصل ({settings.SMALL_FILE_THRESHOLD_MB} MB)"
    )

    print("=" * 60)
    print("[file_router] نتيجة التوجيه (Routing Decision)")
    print(f"    id_run         : {id_run}")
    print(f"    file_path      : {file_path}")
    print(f"    file_size_mb   : {file_size_mb:.2f} MB")
    print(f"    threshold_mb   : {settings.SMALL_FILE_THRESHOLD_MB} MB")
    print(f"    engine_chosen  : {engine}")
    print(f"    reason         : {reason}")
    print("=" * 60)

    return {
        "id_run": id_run,
        "file_path": file_path,
        "file_size_mb": round(file_size_mb, 2),
        "threshold_mb": settings.SMALL_FILE_THRESHOLD_MB,
        "engine": engine,
        "reason": reason,
    }


if __name__ == "__main__":
    # اختبار سريع مستقل للـ Router دون تشغيل باقي الـ Pipeline
    import argparse

    parser = argparse.ArgumentParser(description="اختبار مستقل لـ File Router")
    parser.add_argument("--input", required=True, help="مسار الملف المراد فحصه")
    args = parser.parse_args()

    route_file(args.input)