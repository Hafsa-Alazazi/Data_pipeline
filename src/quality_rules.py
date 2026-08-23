

import json
import re
from datetime import datetime


ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
LATIN_DIGITS = "0123456789"


ARABIC_TO_LATIN_DIGITS = str.maketrans(
    ARABIC_DIGITS + "٫٬",
    LATIN_DIGITS + ".,",
)


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


CURRENCY_MAP = {
    "لاير": "YER", "لاير يمني": "YER", "ريال يمني": "YER", "ر.ي": "YER",
    "yer": "YER", "YER": "YER",
    "دولار": "USD", "$": "USD", "usd": "USD",
    "سعودي": "SAR", "ريال سعودي": "SAR", "sar": "SAR",
}


STATUS_SYNONYMS = {
    "مؤكد": "confirmed", "تم التأكيد": "confirmed", "confirmed": "confirmed",
    "مدفوع": "paid", "تم الدفع": "paid", "paid": "paid",
    "ملغي": "cancelled", "ملغى": "cancelled", "cancelled": "cancelled", "canceled": "cancelled",
    "قيد الانتظار": "pending", "معلق": "pending", "pending": "pending",
    "مرتجع": "returned", "returned": "returned",
}

DATE_INPUT_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",  
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
]


def strip_bom_keys(record: dict) -> dict:
    
    return {key.lstrip("\ufeff"): value for key, value in record.items()}




def clean_numeric_string(raw_value: str):
    
    if raw_value is None:
        return None, None

    text = str(raw_value).strip()
    if text == "":
        return None, None

    original = text

    
    normalized_text = text.replace("،", "").strip()
    if normalized_text in KNOWN_PRICE_WORDS:
        return float(KNOWN_PRICE_WORDS[normalized_text]), "PRICE_IN_WORDS"

    
    converted = text.translate(ARABIC_TO_LATIN_DIGITS)
    rule_applied = "ARABIC_DIGITS" if converted != text else None

    
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
    
    if raw_value is None:
        return None, None
    text = str(raw_value).strip()
    if text == "":
        return None, None

    key = text.lower()
    
    mapped = CURRENCY_MAP.get(text) or CURRENCY_MAP.get(key)
    if mapped is None:
        return text, None  
    changed = mapped != text
    return mapped, "CURRENCY_NORMALIZED" if changed else None


def normalize_phone(raw_value: str):
    
    if raw_value is None:
        return None, None
    text = str(raw_value).strip()
    if text == "":
        return None, None

    cleaned = re.sub(r"[\s\-]", "", text.translate(ARABIC_TO_LATIN_DIGITS))

    
    match = re.match(r"^(?:\+?967)?0?(7\d{8})$", cleaned)
    if not match:
        return text, None  
    normalized = f"+967{match.group(1)}"
    changed = normalized != text
    return normalized, "PHONE_NORMALIZED" if changed else None


def fix_email(raw_value: str):
    
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



def classify_record(raw_record: dict) -> dict:
    
    record = strip_bom_keys(raw_record)
    corrections = []

    
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

    
    if not items_error and _find_negative_item_reason(items):
        core_errors.append("VALUE_NEGATIVE_AMBIGUOUS")

    if len(core_errors) >= 2:
        return _quarantine_result(record, corrections, "ERRORS_CONFLICTING_MULTIPLE", extra_codes=core_errors)
    if len(core_errors) == 1:
        return _quarantine_result(record, corrections, core_errors[0])

    
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

    
    delivery_cost_val = delivery_cost_new if delivery_cost_new is not None else 0.0
    if delivery_cost_val < 0 or (total_amount_new is not None and total_amount_new < 0):
        return _quarantine_result(record, corrections, "VALUE_NEGATIVE_AMBIGUOUS")

    if total_amount_new is None:
        return _quarantine_result(record, corrections, "PRICE_UNKNOWN")

    
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
    
    return {
        "quality_status": "quarantined",
        "cleaned_record": None,
        "corrections": corrections,
        "quarantine_code": code,
        "quarantine_codes": extra_codes if extra_codes else [code],
    }