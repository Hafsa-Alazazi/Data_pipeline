

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

    
    router_ctx = route_file(input_path)

    
    setup_collections()

    
    if router_ctx["engine"] == "python_batch":
        from src.batch_loader import load_batches
        load_summary = load_batches(input_path, router_ctx)
    else:
        
        from src.spark_loader import load_spark
        load_summary = load_spark(input_path, router_ctx)

    
    elt_summary = run_elt(router_ctx["id_run"])

   
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
        
        print(f"[main] ❌ فشل التحقق من الاتساق: {exc}")
        sys.exit(1)
    except Exception as exc:
        # شبكة أمان أخيرة: أي خطأ غير متوقع (مثل عدم تطابق أعمدة CSV مع
        # الـ Schema الثابتة عند اختبار المشروع على ملف بيانات من مصدر
        # آخر - راجع enforceSchema في src/spark_loader.py) يُطبَع برسالة
        # واضحة بدل traceback خام طويل، مع توضيح السبب الأرجح.
        print(f"[main] ❌ خطأ غير متوقع أثناء التنفيذ: {type(exc).__name__}: {exc}")
        print(
            "[main] تلميح: لو الخطأ متعلق بأعمدة CSV، تأكد أن ملف الإدخال "
            "يحتوي نفس أسماء وترتيب الأعمدة الموثقة في README.md."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()