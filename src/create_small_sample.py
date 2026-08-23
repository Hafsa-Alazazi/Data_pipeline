"""
src/create_small_sample.py
----------------------------
يستخرج عينة صغيرة قابلة لإعادة الإنتاج من ملف CSV الضخم، دون تحميل الملف
الأصلي بالكامل إلى الذاكرة في أي لحظة (قراءة Streaming سطرًا بسطر فقط).

الاستخدام:
    python src/create_small_sample.py --input data/orders_huge_mixed_quality.csv --rows 100000

    أو تحديد مسار الإخراج يدويًا:
    python src/create_small_sample.py --input data/orders_huge_mixed_quality.csv \
        --rows 50000 --output data/orders_sample_small.csv

ملاحظة: يمنع إنشاء العينة يدويًا عبر Excel أو أي أداة تحرير يدوية؛ هذا
السكربت هو الطريقة الوحيدة المعتمدة والقابلة لإعادة التنفيذ (Reproducible).
"""

import argparse
import csv
import os
import sys
import time

# السماح باستيراد config عند تشغيل الملف مباشرة من مجلد src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def create_small_sample(input_path: str, output_path: str, n_rows: int) -> dict:
    """
    يقرأ ملف CSV الضخم سطرًا بسطر (Streaming) ويكتب أول n_rows سطر (بعد الهيدر)
    إلى ملف جديد. لا يتم في أي لحظة تحميل الملف كاملاً إلى الذاكرة.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"الملف غير موجود: {input_path}")

    start_time = time.time()
    rows_written = 0

    with open(input_path, "r", encoding=settings.CSV_ENCODING, newline="") as infile, \
         open(output_path, "w", encoding=settings.CSV_ENCODING, newline="") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # كتابة الهيدر كما هو (أول سطر فقط)
        header = next(reader)
        writer.writerow(header)

        for row in reader:
            if rows_written >= n_rows:
                break
            writer.writerow(row)
            rows_written += 1

            if rows_written % 20000 == 0:
                print(f"[create_small_sample] تمت كتابة {rows_written:,} صف حتى الآن...")

    elapsed = time.time() - start_time
    output_size_mb = os.path.getsize(output_path) / (1024 * 1024)

    summary = {
        "input_path": input_path,
        "output_path": output_path,
        "rows_requested": n_rows,
        "rows_written": rows_written,
        "output_size_mb": round(output_size_mb, 2),
        "seconds_elapsed": round(elapsed, 2),
    }
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="إنشاء عينة صغيرة قابلة لإعادة الإنتاج من ملف بيانات الطلبات الضخم."
    )
    parser.add_argument("--input", required=True, help="مسار ملف CSV الضخم المصدر")
    parser.add_argument("--rows", type=int, default=100000, help="عدد الصفوف المطلوب استخراجها")
    parser.add_argument(
        "--output",
        default=None,
        help="مسار ملف الإخراج (اختياري، افتراضيًا داخل مجلد data/)",
    )
    args = parser.parse_args()

    output_path = args.output or os.path.join(
        settings.DATA_DIR, f"orders_sample_{args.rows}.csv"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print(f"[create_small_sample] قراءة من: {args.input}")
    print(f"[create_small_sample] عدد الصفوف المطلوب: {args.rows:,}")

    summary = create_small_sample(args.input, output_path, args.rows)

    print("[create_small_sample] تم الانتهاء بنجاح ✅")
    for key, value in summary.items():
        print(f"    {key}: {value}")


if __name__ == "__main__":
    main()
