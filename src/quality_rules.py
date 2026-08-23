"""
src/quality_rules.py
----------------------
قواعد التنظيف الآلي (8 قواعد على الأقل حسب القسم 6.6)، بالإضافة إلى
دالة تصنيف السجل الكامل إلى Valid / Corrected / Quarantine (القسم 6.4/6.8)
مع بناء أثر التصحيح (Audit Trail) بصيغة القسم 6.7 حرفيًا:

    {
        "field": "...",
        "original_value": "...",
        "corrected_value": "...",
        "rule_code": "..."
    }

مبدأ التصميم: كل قاعدة دالة نقية (Pure Function) لا تتصل بقاعدة البيانات
ولا تطبع شيئًا؛ هذا الفصل (Separation of Concerns) مطلوب صراحة في القسم 9:
"يجب فصل منطق التحميل عن منطق التنظيف عن منطق الاتصال بقاعدة البيانات".

لا تُصحَّح أي قيمة إلا عندما تكون قاعدة التحويل واضحة (بدون تخمين)، كما
ينص القسم 6.6 صراحة. أي حالة غامضة تُعزل بدل تخمين قيمة قد تكون خاطئة.
"""

import json
import re
from datetime import datetime

# --------------------------------------------------------------------------
# ثوابت وقواميس التطبيع
# --------------------------------------------------------------------------

ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
LATIN_DIGITS = "0123456789"

# ملاحظة مهمة (إصلاح): الأرقام العربية أحيانًا تُكتب مع فاصلة عشرية عربية
# ٫ (U+066B) أو فاصلة آلاف عربية ٬ (U+066C)، وهما ليسا من ضمن "٠-٩" فلا
# يتأثران بـ translate القديم. كانت هذه الفاصلة تُحذف بصمت لاحقًا بالـ regex
# بدل ما تتحول لنقطة عشرية، فتتحول القيمة "٧٠٦٠٠٠٫٠" (=706000.0) خطأً
# إلى 7060000.0 (×10). لذلك نضيف تحويلها صراحة ضمن نفس جدول الترجمة.
ARABIC_TO_LATIN_DIGITS = str.maketrans(
    ARABIC_DIGITS + "٫٬",
    LATIN_DIGITS + ".,",
)

# كلمات السعر المعروفة فقط (بدون تخمين لأي كلمة غير موجودة بالقاموس)
KNOWN_PRICE_WORDS = {
    "ألف": 1000, "الف": 1000,
    "ألفان": 2000, "الفان": 2000,
    "ألفين": 2000, "الفين": 2000,
    "ثلاثة آلاف": 3000, "ثلاثة الاف": 3000,
    "أربعة آلاف": 4000, "اربعة الاف": 4000,
    "خمسة آلاف": 5000, "خمسة الاف": 5000,
    "ستة آلاف": 6000, "ستة الاف": 6000,
    "سبعة آلاف": 7000, "سبعة الاف": 7000,
    "ثمانية آلاف": 8000, "ثمانية الاف": 8000,
    "تسعة آلاف": 9000, "تسعة الاف": 9000,
    "عشرة آلاف": 10000, "عشرة الاف": 10000,
}

# رموز/أسماء العملة المعروفة → توحيدها إلى YER (الحقل currency في ملفك)
CURRENCY_MAP = {
    "لاير": "YER", "لاير يمني": "YER", "ريال يمني": "YER", "ر.ي": "YER",
    "yer": "YER", "YER": "YER",
    "دولار": "USD", "$": "USD", "usd": "USD",
    "سعودي": "SAR", "ريال سعودي": "SAR", "sar": "SAR",
}

# قاموس توحيد الحالات النصية (status / payment_status) بعد Trim
STATUS_SYNONYMS = {
    "مؤكد": "confirmed", "تم التأكيد": "confirmed", "confirmed": "confirmed",
    "مدفوع": "paid", "تم الدفع": "paid", "paid": "paid",
    "ملغي": "cancelled", "ملغى": "cancelled", "cancelled": "cancelled", "canceled": "cancelled",
    "قيد الانتظار": "pending", "معلق": "pending", "pending": "pending",
    "مرتجع": "returned", "returned": "returned",
}

DATE_INPUT_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",  # 2025-02-24T21:29:00 (الصيغة الفعلية في الملف)
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
]


def strip_bom_keys(record: dict) -> dict:
    """
    يزيل رمز BOM (\\ufeff) من أسماء المفاتيح إن وُجد، لتفادي مشكلة
    الحقول التي تحمل هذا الرمز الخفي (مثل 'order_id' التي تصل أحيانًا
    كـ '\\ufefforder_id' بسبب ترميز الملف الأصلي).
    """
    return {key.lstrip("\ufeff"): value for key, value in record.items()}


# --------------------------------------------------------------------------
# القواعد الفردية (Field-Level Rules)
# كل دالة تُعيد Tuple: (القيمة النهائية, تغيّرت؟, رمز_القاعدة أو None)
# --------------------------------------------------------------------------

def clean_numeric_string(raw_value: str):
    """
    يجمع 3 قواعد من الجدول لأنها تُطبَّق بنفس التسلسل على أي حقل رقمي نصي
    (delivery_cost, payment_amount, total_amount):
      - الأرقام العربية → لاتينية        (rule_code: ARABIC_DIGITS)
      - فواصل الآلاف                      (rule_code: THOUSAND_SEPARATOR)
      - السعر بالكلمات (قائمة معروفة فقط) (rule_code: PRICE_IN_WORDS)

    يعيد (float | None, rule_code | None). None يعني تعذر التحويل بأمان.
    """
    if raw_value is None:
        return None, None

    text = str(raw_value).strip()
    if text == "":
        return None, None

    original = text

    # 1) السعر بالكلمات: تحقق أولاً من القاموس المعروف فقط (بدون تخمين)
    normalized_text = text.replace("،", "").strip()
    if normalized_text in KNOWN_PRICE_WORDS:
        return float(KNOWN_PRICE_WORDS[normalized_text]), "PRICE_IN_WORDS"

    # 2) الأرقام العربية → لاتينية
    converted = text.translate(ARABIC_TO_LATIN_DIGITS)
    rule_applied = "ARABIC_DIGITS" if converted != text else None

    # 3) إزالة أي نص غير رقمي (عملة، مسافات) وفواصل الآلاف
    cleaned = re.sub(r"[^\d.\-]", "", converted.replace(",", ""))
    if "," in converted or re.search(r"[^\d.\-,]", converted.replace(",", "")):
        rule_applied = "THOUSAND_SEPARATOR" if "," in converted else rule_applied

    if cleaned in ("", "-", "."):
        return None, None

    try:
        value = float(cleaned)
    except ValueError:
        return None, None

    if rule_applied is None and cleaned != original:
        rule_applied = "NUMERIC_CLEANUP"

    return value, rule_applied


def normalize_currency(raw_value: str):
    """توحيد رمز/اسم العملة إلى صيغة قياسية (مثل YER)."""
    if raw_value is None:
        return None, None
    text = str(raw_value).strip()
    if text == "":
        return None, None

    key = text.lower()
    # نبحث أولاً بالمطابقة الحرفية العربية، ثم بالحروف الصغيرة الإنجليزية
    mapped = CURRENCY_MAP.get(text) or CURRENCY_MAP.get(key)
    if mapped is None:
        return text, None  # لا نخمّن عملة غير معروفة؛ نتركها كما هي (قد تُعزل لاحقًا لو أثّرت)
    changed = mapped != text
    return mapped, "CURRENCY_NORMALIZED" if changed else None


def normalize_phone(raw_value: str):
    """إزالة المسافات وتوحيد صيغة رقم الهاتف عند وضوحها فقط."""
    if raw_value is None:
        return None, None
    text = str(raw_value).strip()
    if text == "":
        return None, None

    cleaned = re.sub(r"[\s\-]", "", text.translate(ARABIC_TO_LATIN_DIGITS))

    # صيغة واضحة فقط: يبدأ بـ +967 أو 967 أو 0 متبوعًا بأرقام يمنية معروفة (7 أرقام محلية)
    match = re.match(r"^(?:\+?967)?0?(7\d{8})$", cleaned)
    if not match:
        return text, None  # صيغة غير واضحة؛ لا نخمّن، نتركها لتُقيَّم لاحقًا

    normalized = f"+967{match.group(1)}"
    changed = normalized != text
    return normalized, "PHONE_NORMALIZED" if changed else None


def fix_email(raw_value: str):
    """
    يصلح فقط التكرار الواضح في البريد الإلكتروني (@@ أو .. مكررة)، حسب
    نص القسم 6.6: "إصلاح التكرار الواضح فقط؛ وإلا يعزل السجل".
    يعيد (email_مصحح, rule_code) أو (email, None) إذا كان سليمًا أصلاً،
    أو (None, "UNFIXABLE") إذا كان تالفًا بشكل لا يمكن إصلاحه بأمان.
    """
    if raw_value is None:
        return None, "UNFIXABLE"
    text = str(raw_value).strip()
    if text == "":
        return None, "UNFIXABLE"

    fixed = re.sub(r"@{2,}", "@", text)
    fixed = re.sub(r"\.{2,}", ".", fixed)

    email_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    if not re.match(email_pattern, fixed):
        return None, "UNFIXABLE"

    changed = fixed != text
    return fixed, "EMAIL_REPEATED_SYMBOLS" if changed else None


def normalize_date(raw_value: str):
    """
    يحوّل التاريخ إلى صيغة قياسية ISO (YYYY-MM-DD) عند وضوح الصيغة، ويتحقق
    أنه تاريخ منطقي فعلاً (وليس فقط شكلاً صحيحًا، مثل 2025-13-40).
    يعيد (date_str, rule_code) أو (None, "DATE_IMPOSSIBLE_INVALID").
    """
    if raw_value is None:
        return None, "DATE_IMPOSSIBLE_INVALID"
    text = str(raw_value).strip().translate(ARABIC_TO_LATIN_DIGITS)
    if text == "":
        return None, "DATE_IMPOSSIBLE_INVALID"

    for fmt in DATE_INPUT_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)
            standardized = parsed.strftime("%Y-%m-%dT%H:%M:%S")
            changed = standardized != text
            return standardized, "DATE_NORMALIZED" if changed else None
        except ValueError:
            continue

    return None, "DATE_IMPOSSIBLE_INVALID"


def normalize_status_text(raw_value: str):
    """Trim وتوحيد القيم النصية (status/payment_status) إلى قاموس قياسي."""
    if raw_value is None:
        return None, None
    text = str(raw_value).strip()
    if text == "":
        return None, None

    mapped = STATUS_SYNONYMS.get(text) or STATUS_SYNONYMS.get(text.lower())
    if mapped is None:
        return text, "TRIMMED" if text != str(raw_value) else None

    changed = mapped != text
    return mapped, "STATUS_NORMALIZED" if changed else ("TRIMMED" if text != str(raw_value) else None)


def parse_items_json(raw_value):
    """
    يتحقق من صحة items_json ويحلله. يعيد (items_list, error_code|None).
    error_code من: JSON_ITEMS_CORRUPTED أو ITEMS_EMPTY.
    """
    if raw_value is None or str(raw_value).strip() == "":
        return None, "JSON_ITEMS_CORRUPTED"
    try:
        items = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError):
        return None, "JSON_ITEMS_CORRUPTED"

    if not isinstance(items, list) or len(items) == 0:
        return None, "ITEMS_EMPTY"

    return items, None


def _find_negative_item_reason(items: list):
    """
    إصلاح: كانت السجلات التي تحتوي كمية أو سعر سالب داخل items_json
    (مثل "qty": -2) تمر بدون عزل إطلاقًا، لأن فحص VALUE_NEGATIVE_AMBIGUOUS
    القديم كان يتحقق فقط من delivery_cost و total_amount على مستوى
    السجل الكامل، وليس من داخل مصفوفة items. نتيجة الاختبار على العيّنة:
    674 سجل فيه qty سالبة كانت تُصنَّف Valid/Corrected خطأً بدل Quarantine.

    تفحص كل عنصر (unit_price/price و qty/quantity)، وتعيد True إذا وُجدت
    قيمة سالبة واضحة في أي عنصر (لا يمكن تحديد معناها بأمان: هل هي مرتجع؟
    خطأ إدخال؟ لا نخمّن). القيم غير الرقمية أو المفقودة تُترك لمراحل
    فحص أخرى (PRICE_UNKNOWN / JSON غير صالح) بدل اعتبارها سالبة.
    """
    for item in items:
        if not isinstance(item, dict):
            continue
        for raw_value in (
            item.get("unit_price", item.get("price")),
            item.get("qty", item.get("quantity")),
        ):
            if raw_value is None:
                continue
            try:
                if float(raw_value) < 0:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def recompute_total(items: list, delivery_cost: float):
    """
    يعيد حساب إجمالي الطلب من مجموع العناصر + التوصيل، فقط إذا كانت كل
    عناصر items تحتوي سعر وكمية رقميين صالحين (مكوّنات صالحة).
    يعيد (total | None, rule_applied: bool).

    إصلاح: الحقول الفعلية في items_json بملف المشروع هي unit_price وqty
    (وليس price وquantity كما كان بالكود الأصلي)، لذلك كانت هذه الدالة
    تفشل صامتة على كل سجل (float(None) يرمي TypeError دائمًا) ولا تُطبَّق
    قاعدة TOTAL_RECOMPUTED إطلاقًا رغم وجودها بالكود. نقرأ الاسمين معًا
    (unit_price أولًا، وprice كبديل احتياطي) لتغطية أي مصدر بيانات مشابه.

    ملاحظة: فحص القيم السالبة داخل العناصر لم يعد مسؤولية هذه الدالة؛
    أصبح يُفحص مبكرًا في classify_record عبر _find_negative_item_reason
    قبل الوصول لهذه المرحلة أصلًا (انظر إصلاح #3)، فإن وصلنا هنا فالعناصر
    غير سالبة، لكن نُبقي الفحص هنا أيضًا كخط دفاع ثانٍ (Defense in Depth).
    """
    try:
        items_sum = 0.0
        for item in items:
            price_raw = item.get("unit_price", item.get("price"))
            qty_raw = item.get("qty", item.get("quantity", 1))
            price = float(price_raw)
            qty = float(qty_raw)
            if price < 0 or qty < 0:
                return None, False
            items_sum += price * qty
        total = items_sum + (delivery_cost or 0)
        return round(total, 2), True
    except (TypeError, ValueError, AttributeError):
        return None, False


# --------------------------------------------------------------------------
# دالة التصنيف الرئيسية للسجل الكامل
# --------------------------------------------------------------------------

def classify_record(raw_record: dict) -> dict:
    """
    تُطبَّق على سجل واحد من orders_raw (حقل record_raw تحديدًا)، وتُعيد
    قاموسًا موحّدًا يحتوي:
        {
            "quality_status": "valid" | "corrected" | "quarantined",
            "cleaned_record": {...} أو None إن كان معزولاً بالكامل,
            "corrections": [ {field, original_value, corrected_value, rule_code}, ... ],
            "quarantine_code": "..." أو None,
        }

    الترتيب المنطقي:
      1. فحوصات جوهرية أولاً (تُسبب عزلاً فوريًا بدون محاولة تصحيح):
         id_order مفقود، id_customer مفقود، items_json تالف/فارغ، كمية/سعر
         سالب داخل items. هذه الفحوصات الأربعة تُجمع أولًا بالكامل (بدل
         التوقف عند أول واحدة)؛ فإن اجتمع أكثر من سبب بنفس السجل يُستخدم
         ERRORS_CONFLICTING_MULTIPLE (إصلاح: كان هذا الرمز موجودًا بالجدول
         والقاموس لكن لا يظهر أبدًا عمليًا لأن الكود القديم يتوقف عند أول
         خطأ ولا يتحقق من الباقي).
      2. تطبيق قواعد التصحيح القابلة للأتمتة على باقي الحقول.
      3. فحص السعر/القيم السالبة الغامضة بعد التصحيح.
      4. تحديد الحالة النهائية: valid إذا لا تصحيحات، corrected إذا وُجدت
         تصحيحات ونجحت كل الفحوصات الجوهرية، quarantined غير ذلك.
    """
    record = strip_bom_keys(raw_record)
    corrections = []

    # ---- 1) فحوصات جوهرية: تُجمع كلها أولاً قبل اتخاذ القرار ----
    core_errors = []

    order_id = str(record.get("order_id") or "").strip()
    if order_id == "":
        core_errors.append("ID_ORDER_MISSING")

    customer_id = str(record.get("customer_id") or "").strip()
    if customer_id == "":
        core_errors.append("ID_CUSTOMER_MISSING")

    items, items_error = parse_items_json(record.get("items_json"))
    if items_error:
        core_errors.append(items_error)

    # فحص الكميات/الأسعار السالبة فقط لو نجح تحليل items أصلًا (وإلا لا
    # معنى لفحص محتوى قائمة غير موجودة)
    if not items_error and _find_negative_item_reason(items):
        core_errors.append("VALUE_NEGATIVE_AMBIGUOUS")

    if len(core_errors) >= 2:
        return _quarantine_result(record, corrections, "ERRORS_CONFLICTING_MULTIPLE", extra_codes=core_errors)
    if len(core_errors) == 1:
        return _quarantine_result(record, corrections, core_errors[0])

    # ---- 2) تطبيق قواعد التصحيح على الحقول القابلة للأتمتة ----
    cleaned = dict(record)

    order_date_new, date_rule = normalize_date(record.get("order_date"))
    if date_rule == "DATE_IMPOSSIBLE_INVALID":
        return _quarantine_result(record, corrections, "DATE_IMPOSSIBLE_INVALID")
    _apply(cleaned, corrections, "order_date", record.get("order_date"), order_date_new, date_rule)

    for field in ["status", "payment_status"]:
        new_val, rule = normalize_status_text(record.get(field))
        _apply(cleaned, corrections, field, record.get(field), new_val, rule)

    phone_new, phone_rule = normalize_phone(record.get("customer_phone"))
    _apply(cleaned, corrections, "customer_phone", record.get("customer_phone"), phone_new, phone_rule)

    email_new, email_rule = fix_email(record.get("customer_email"))
    if email_rule == "UNFIXABLE":
        return _quarantine_result(record, corrections, "EMAIL_UNFIXABLE")
    _apply(cleaned, corrections, "customer_email", record.get("customer_email"), email_new, email_rule)

    currency_new, currency_rule = normalize_currency(record.get("currency"))
    _apply(cleaned, corrections, "currency", record.get("currency"), currency_new, currency_rule)

    delivery_cost_new, delivery_rule = clean_numeric_string(record.get("delivery_cost"))
    _apply(cleaned, corrections, "delivery_cost", record.get("delivery_cost"), delivery_cost_new, delivery_rule)

    payment_amount_new, payment_rule = clean_numeric_string(record.get("payment_amount"))
    _apply(cleaned, corrections, "payment_amount", record.get("payment_amount"), payment_amount_new, payment_rule)

    total_amount_new, total_rule = clean_numeric_string(record.get("total_amount"))
    _apply(cleaned, corrections, "total_amount", record.get("total_amount"), total_amount_new, total_rule)

    # ---- 3) فحص القيم الجوهرية بعد التصحيح ----
    delivery_cost_val = delivery_cost_new if delivery_cost_new is not None else 0.0
    if delivery_cost_val < 0 or (total_amount_new is not None and total_amount_new < 0):
        return _quarantine_result(record, corrections, "VALUE_NEGATIVE_AMBIGUOUS")

    if total_amount_new is None:
        return _quarantine_result(record, corrections, "PRICE_UNKNOWN")

    # إعادة حساب الإجمالي فقط عند عدم تطابقه مع مجموع العناصر + التوصيل
    recomputed_total, recompute_ok = recompute_total(items, delivery_cost_val)
    if recompute_ok and recomputed_total is not None and abs(recomputed_total - total_amount_new) > 0.01:
        _apply(cleaned, corrections, "total_amount", total_amount_new, recomputed_total, "TOTAL_RECOMPUTED")
        total_amount_new = recomputed_total

    cleaned["delivery_cost"] = delivery_cost_val
    cleaned["payment_amount"] = payment_amount_new
    cleaned["total_amount"] = total_amount_new
    cleaned["items"] = items

    quality_status = "corrected" if corrections else "valid"

    return {
        "quality_status": quality_status,
        "cleaned_record": cleaned,
        "corrections": corrections,
        "quarantine_code": None,
    }


def _apply(cleaned: dict, corrections: list, field: str, original, new_value, rule_code):
    """يُحدّث السجل النظيف ويُسجّل التصحيح في Audit Trail إذا تغيّرت القيمة فعليًا."""
    if rule_code is None:
        return
    if new_value is None and original is None:
        return
    if str(new_value) == str(original):
        return
    cleaned[field] = new_value
    corrections.append(
        {
            "field": field,
            "original_value": original,
            "corrected_value": new_value,
            "rule_code": rule_code,
        }
    )


def _quarantine_result(record: dict, corrections: list, code: str, extra_codes: list = None) -> dict:
    """
    quarantine_code: الرمز الأساسي (يبقى دائمًا موجودًا للتوافق مع أي كود
        قديم يقرأ حقلًا واحدًا فقط، مثل elt_pipeline.py).
    quarantine_codes: قائمة بكل أسباب العزل (حقل جديد). في الحالة العادية
        تحتوي عنصرًا واحدًا مطابقًا لـ quarantine_code. عند
        ERRORS_CONFLICTING_MULTIPLE تحتوي كل الأسباب الجوهرية الفعلية
        (مثل ["ID_ORDER_MISSING", "JSON_ITEMS_CORRUPTED"]) بدل فقدانها.
    """
    return {
        "quality_status": "quarantined",
        "cleaned_record": None,
        "corrections": corrections,
        "quarantine_code": code,
        "quarantine_codes": extra_codes if extra_codes else [code],
    }