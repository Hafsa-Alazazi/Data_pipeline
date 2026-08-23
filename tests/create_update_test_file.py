

import argparse
import csv
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from src.mongo_setup import get_database


def pick_existing_validated_order_id() -> str:
   
    db = get_database()
    doc = db[settings.COLLECTION_VALIDATED].find_one({}, {"id_order": 1})
    if not doc:
        raise RuntimeError(
            "orders_validated فارغة حاليًا! يجب تشغيل main.py مرة واحدة على "
            "الأقل على نفس ملف البيانات قبل استخدام سكربت اختبار Update."
        )
    return doc["id_order"]


def create_update_test_file(input_path: str, output_path: str, target_order_id: str) -> dict:
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"الملف غير موجود: {input_path}")

    old_value = None
    new_value = None
    found = False

    with open(input_path, "r", encoding="utf-8-sig", newline="") as infile, \
         open(output_path, "w", encoding="utf-8-sig", newline="") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            if not found and row.get("order_id") == target_order_id:
                old_value = row.get("payment_status")
                
                new_value = "قيد الانتظار" if old_value != "قيد الانتظار" else "مؤكد"
                row["payment_status"] = new_value
                found = True

            writer.writerow(row)

    if not found:
        raise RuntimeError(
            f"لم يُعثر على order_id={target_order_id} داخل الملف {input_path}. "
            "تأكد أن الملف نفسه الذي شُغّل عليه main.py سابقًا."
        )

    return {
        "modified_order_id": target_order_id,
        "field_changed": "payment_status",
        "old_value": old_value,
        "new_value": new_value,
        "output_path": output_path,
    }


def main():
    parser = argparse.ArgumentParser(
        description="إنشاء ملف اختبار به سجل واحد معدَّل (مؤكد وجوده في orders_validated)، لإثبات مسار Update في Upsert."
    )
    parser.add_argument("--input", required=True, help="مسار ملف CSV الأصلي")
    parser.add_argument("--output", required=True, help="مسار ملف الإخراج المعدَّل")
    args = parser.parse_args()

    print("[create_update_test_file] البحث عن id_order موجود فعليًا في orders_validated...")
    target_order_id = pick_existing_validated_order_id()
    print(f"[create_update_test_file] تم اختيار: {target_order_id}")

    result = create_update_test_file(args.input, args.output, target_order_id)

    print("[create_update_test_file] تم إنشاء ملف اختبار Update بنجاح:")
    for key, value in result.items():
        print(f"    {key}: {value}")
    print(
        "\n[create_update_test_file] الخطوة التالية: "
        f"python main.py --input {result['output_path']}"
    )
    print(
        "[create_update_test_file] المتوقع في النتائج: count_updated=1، count_inserted=0، "
        "count_unchanged = (عدد valid+corrected من آخر تشغيل) - 1."
    )


if __name__ == "__main__":
    main()