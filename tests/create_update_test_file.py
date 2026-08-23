"""
tests/create_update_test_file.py
----------------------------------
سكربت اختبار مستقل (وليس تعديلاً يدويًا) يُنتج نسخة من ملف العينة بعد
تعديل حقل واحد فقط في سجل **مؤكد وجوده فعليًا** في orders_validated،
للاستخدام في إثبات مسار Update ضمن Upsert (القسم 6.10 من الوثيقة).

إصلاح مهم: النسخة الأولى من هذا السكربت كانت تختار "أول سجل عنده
order_id و customer_id غير فارغين" كهدف للتعديل، بافتراض أن هذا يكفي
ليكون السجل "صالحًا". هذا افتراض خاطئ: قد يكون هذا السجل نفسه معزولاً
لسبب آخر تمامًا (مثل كمية سالبة داخل items، أو تاريخ مستحيل...)، وبالتالي
تعديل حقل فيه لن يظهر أي أثر على orders_validated لأنه أصلاً غير موجود
فيها (لا قبل التعديل ولا بعده).

الحل: نسأل MongoDB مباشرة عن id_order موجود فعليًا داخل orders_validated
(أي سجل نجح فعلاً في اجتياز كل الفحوصات الجوهرية)، ثم نبحث عن هذا
الـ order_id بالذات داخل ملف CSV الأصلي ونُعدّل حقل payment_status له
فقط - هذا يضمن أن السجل المعدَّل موجود مسبقًا في orders_validated فعليًا
قبل التعديل.

لماذا سكربت وليس تعديل يدوي بـ Excel: نفس المبرر السابق - هذا اختبار
متحكَّم به وقابل لإعادة التنفيذ (Reproducible)، وليس تعديلاً يدويًا
"لتسهيل النتائج" (الممنوع في القسم 9 و13).

الاستخدام (بعد تشغيل main.py مرة واحدة على الأقل على نفس البيانات، حتى
تكون orders_validated غير فارغة):
    python tests/create_update_test_file.py \
        --input data/orders_sample_100000.csv \
        --output data/orders_update_test.csv

ثم:
    python main.py --input data/orders_update_test.csv

النتيجة المتوقعة بدقة: count_updated = 1، count_inserted = 0،
count_unchanged = (عدد valid+corrected من آخر تشغيل) - 1.
"""

import argparse
import csv
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from src.mongo_setup import get_database


def pick_existing_validated_order_id() -> str:
    """
    يسأل MongoDB مباشرة عن أي id_order موجود فعليًا في orders_validated.
    هذا يضمن أن السجل الذي سنعدّله ليس معزولاً، بعكس النسخة الأولى من
    هذا السكربت التي كانت تخمّن بدون التحقق من قاعدة البيانات.
    """
    db = get_database()
    doc = db[settings.COLLECTION_VALIDATED].find_one({}, {"id_order": 1})
    if not doc:
        raise RuntimeError(
            "orders_validated فارغة حاليًا! يجب تشغيل main.py مرة واحدة على "
            "الأقل على نفس ملف البيانات قبل استخدام سكربت اختبار Update."
        )
    return doc["id_order"]


def create_update_test_file(input_path: str, output_path: str, target_order_id: str) -> dict:
    """
    ينسخ الملف كاملاً سطرًا بسطر (Streaming)، ويُعدّل حقل payment_status
    فقط لسطر order_id == target_order_id (السجل المؤكد وجوده مسبقًا في
    orders_validated)، ويترك كل شيء آخر كما هو تمامًا.
    """
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
                # قيمة معروفة ضمن STATUS_SYNONYMS في quality_rules.py، لضمان
                # أن السجل يبقى صالحًا/مصححًا بعد التعديل ولا يُعزل بسبب جديد.
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