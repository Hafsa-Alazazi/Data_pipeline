"""
main.py
--------
نقطة التشغيل الموحدة الوحيدة للمشروع (القسم 6.2 و 9 من الوثيقة).

هذا الملف لا يحتوي أي منطق معالجة أو تنظيف بنفسه؛ فقط "ينسّق" استدعاء
المراحل بالترتيب الصحيح حسب معمارية القسم 4:

    1) File Discovery + Engine Selection  -> src/file_router.py
    2) Load Raw (Batch أو PySpark حسب قرار Router) -> src/batch_loader.py / src/spark_loader.py
    3) Quality & Transform + Classification + Final Load (Upsert) -> src/elt_pipeline.py
    4) Metrics -> src/metrics.py -> reports/results.json

الاستخدام:
    python main.py --input data/orders_sample_100000.csv
    python main.py --input data/orders_huge_mixed_quality.csv

لإثبات Idempotency (القسم 6.10): شغّل نفس الأمر مرتين متتاليتين بنفس
--input، وقارن count_inserted/count_updated/count_unchanged بين
التشغيلين في reports/results.json (يجب أن يكون التشغيل الثاني
count_inserted=0 وcount_unchanged = عدد سجلات validated من التشغيل الأول).
"""

import argparse
import sys
import time

from config import settings
from src.file_router import route_file
from src.mongo_setup import setup_collections
from src.elt_pipeline import run_elt
from src.metrics import build_metrics_record, save_metrics, print_metrics_summary


def run_pipeline(input_path: str) -> dict:
    pipeline_start = time.time()

    print("#" * 60)
    print("# بدء تشغيل خط البيانات الهجين")
    print("#" * 60)

    # ---- المرحلة 1: File Discovery + Engine Selection ----
    router_ctx = route_file(input_path)

    # ---- إعداد المجموعات والفهارس (عملية آمنة للتكرار - idempotent) ----
    # create_index لا يعيد إنشاء الفهرس لو كان موجودًا مسبقًا بنفس المواصفات،
    # لذلك تشغيلها في بداية كل main.py آمن تمامًا ولا يكسر شيئًا.
    setup_collections()

    # ---- المرحلة 2: Load Raw (المحرك يُحدَّد تلقائيًا من قرار Router) ----
    if router_ctx["engine"] == "python_batch":
        from src.batch_loader import load_batches
        load_summary = load_batches(input_path, router_ctx)
    else:
        # استيراد مؤجّل: PySpark ثقيل الإقلاع، لا داعي لتحميله إلا عند
        # الحاجة الفعلية (ملف كبير فقط).
        from src.spark_loader import load_spark
        load_summary = load_spark(input_path, router_ctx)

    # ---- المرحلة 3: Quality & Transform + Classification + Upsert ----
    elt_summary = run_elt(router_ctx["id_run"])

    # ---- المرحلة 4: Metrics ----
    metrics_record = build_metrics_record(router_ctx, load_summary, elt_summary)
    results_path = save_metrics(metrics_record)
    print_metrics_summary(metrics_record)

    total_elapsed = round(time.time() - pipeline_start, 2)
    print(f"[main] ✅ اكتمل التشغيل الكامل خلال {total_elapsed} ثانية.")
    print(f"[main] النتائج محفوظة في: {results_path}")
    print(f"[main] id_run لهذا التشغيل: {router_ctx['id_run']}")

    return metrics_record


def main():
    parser = argparse.ArgumentParser(
        description="تشغيل خط البيانات الهجين الكامل (Router -> Load -> ELT -> Metrics)."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="مسار ملف CSV المراد معالجته (صغير أو كبير - المحرك يُختار تلقائيًا).",
    )
    args = parser.parse_args()

    try:
        run_pipeline(args.input)
    except FileNotFoundError as exc:
        print(f"[main] ❌ خطأ: {exc}")
        sys.exit(1)
    except ConnectionError as exc:
        print(f"[main] ❌ تعذّر الاتصال بقاعدة البيانات: {exc}")
        sys.exit(1)
    except AssertionError as exc:
        # فشل التحقق من معادلة الاتساق (القسم 6.11) - خطأ جوهري في البيانات
        # أو في منطق التصنيف، يجب أن يوقف التشغيل بوضوح وليس صامتًا.
        print(f"[main] ❌ فشل التحقق من الاتساق: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()