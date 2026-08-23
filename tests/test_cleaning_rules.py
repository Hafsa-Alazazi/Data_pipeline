"""
tests/test_cleaning_rules.py
------------------------------
اختبارات وحدة (Unit Tests) لكل قاعدة تنظيف فردية في src/quality_rules.py،
تُشغَّل عبر pytest:

    pytest tests/test_cleaning_rules.py -v

تغطي القواعد الثمانية+ المطلوبة صراحة في القسم 6.6 من الوثيقة، كل قاعدة
بحالة "يجب أن تُصحَّح" وحالة "غامضة يجب ألا تُخمَّن".
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.quality_rules import (
    clean_numeric_string,
    normalize_currency,
    normalize_phone,
    fix_email,
    normalize_date,
    normalize_status_text,
    parse_items_json,
    strip_bom_keys,
)


# --------------------------------------------------------------------------
# 1) الأرقام العربية + فواصل الآلاف + السعر بالكلمات (clean_numeric_string)
# --------------------------------------------------------------------------

def test_arabic_digits_converted_to_latin():
    value, rule = clean_numeric_string("٥٠٠٠")
    assert value == 5000.0
    assert rule == "ARABIC_DIGITS"


def test_thousand_separator_removed():
    value, rule = clean_numeric_string("125,000.00")
    assert value == 125000.00
    assert rule == "THOUSAND_SEPARATOR"


def test_arabic_decimal_comma_not_multiplied_by_ten():
    """
    اختبار انحدار (Regression Test) لإصلاح حقيقي حصل أثناء التطوير:
    "٧٠٦٠٠٠٫٠" (فاصلة عشرية عربية ٫) كانت تتحول خطأً إلى 7060000.0 بدل
    706000.0 لأن ٫ لم تكن ضمن جدول ترجمة الأرقام العربية القديم.
    """
    value, rule = clean_numeric_string("٧٠٦٠٠٠٫٠")
    assert value == 706000.0


def test_known_price_word_converted():
    value, rule = clean_numeric_string("خمسة آلاف")
    assert value == 5000.0
    assert rule == "PRICE_IN_WORDS"


def test_unknown_price_word_not_guessed():
    """كلمة سعر غير موجودة بالقاموس المعروف يجب ألا تُخمَّن أبدًا."""
    value, rule = clean_numeric_string("كمية كبيرة جدًا")
    assert value is None


def test_empty_numeric_string_returns_none():
    value, rule = clean_numeric_string("")
    assert value is None
    assert rule is None


# --------------------------------------------------------------------------
# 2) رمز/اسم العملة (normalize_currency)
# --------------------------------------------------------------------------

def test_currency_yemeni_rial_name_normalized():
    value, rule = normalize_currency("لاير يمني")
    assert value == "YER"
    assert rule == "CURRENCY_NORMALIZED"


def test_currency_already_normalized_no_rule_applied():
    value, rule = normalize_currency("YER")
    assert value == "YER"
    assert rule is None  # لم تتغيّر القيمة، فلا داعي لتسجيل تصحيح


def test_unknown_currency_not_guessed():
    """عملة غير معروفة يجب أن تُترك كما هي دون تخمين."""
    value, rule = normalize_currency("عملة غريبة")
    assert value == "عملة غريبة"
    assert rule is None


# --------------------------------------------------------------------------
# 3) رقم الهاتف (normalize_phone)
# --------------------------------------------------------------------------

def test_phone_with_country_code_and_spaces_normalized():
    value, rule = normalize_phone("+967 77 123 4567")
    assert value == "+967771234567"
    assert rule == "PHONE_NORMALIZED"


def test_phone_local_format_normalized():
    value, rule = normalize_phone("0771234567")
    assert value == "+967771234567"


def test_phone_ambiguous_format_left_unchanged():
    """رقم غير واضح الصيغة يجب ألا يُخمَّن، يُترك كما هو."""
    value, rule = normalize_phone("12345")
    assert value == "12345"
    assert rule is None


# --------------------------------------------------------------------------
# 4) البريد الإلكتروني (fix_email)
# --------------------------------------------------------------------------

def test_email_repeated_at_symbol_fixed():
    value, rule = fix_email("user@@mail.com")
    assert value == "user@mail.com"
    assert rule == "EMAIL_REPEATED_SYMBOLS"


def test_email_repeated_dot_fixed():
    value, rule = fix_email("user@mail..com")
    assert value == "user@mail.com"
    assert rule == "EMAIL_REPEATED_SYMBOLS"


def test_email_already_valid_unchanged():
    value, rule = fix_email("user@mail.com")
    assert value == "user@mail.com"
    assert rule is None


def test_email_missing_at_symbol_unfixable():
    """بريد بدون @ إطلاقًا لا يمكن إصلاحه بأمان، يجب أن يُعزل."""
    value, rule = fix_email("usermail.com")
    assert value is None
    assert rule == "UNFIXABLE"


def test_email_none_is_unfixable():
    value, rule = fix_email(None)
    assert value is None
    assert rule == "UNFIXABLE"


# --------------------------------------------------------------------------
# 5) التاريخ (normalize_date)
# --------------------------------------------------------------------------

def test_date_iso_format_normalized():
    value, rule = normalize_date("2025-01-31")
    assert value == "2025-01-31T00:00:00"


def test_date_slash_format_normalized():
    value, rule = normalize_date("2025/01/31")
    assert value == "2025-01-31T00:00:00"


def test_date_impossible_day_rejected():
    """تاريخ شكليًا يشبه تاريخًا لكنه غير منطقي فعليًا (لا يوجد يوم 40)."""
    value, rule = normalize_date("2025-13-40")
    assert value is None
    assert rule == "DATE_IMPOSSIBLE_INVALID"


def test_date_none_is_impossible():
    value, rule = normalize_date(None)
    assert rule == "DATE_IMPOSSIBLE_INVALID"


# --------------------------------------------------------------------------
# 6) المسافات والمرادفات (normalize_status_text)
# --------------------------------------------------------------------------

def test_status_synonym_mapped_to_standard_dictionary():
    value, rule = normalize_status_text("تم الدفع")
    assert value == "paid"
    assert rule == "STATUS_NORMALIZED"


def test_status_already_standard_no_change():
    value, rule = normalize_status_text("paid")
    assert value == "paid"
    assert rule is None


def test_status_unknown_value_left_as_is():
    """قيمة غير موجودة بالقاموس القياسي تُترك كما هي (بدون تخمين)."""
    value, rule = normalize_status_text("حالة غير معروفة")
    assert value == "حالة غير معروفة"


# --------------------------------------------------------------------------
# 7) items_json (parse_items_json)
# --------------------------------------------------------------------------

def test_items_json_valid_parsed_correctly():
    items, error = parse_items_json('[{"unit_price": 100, "qty": 2}]')
    assert error is None
    assert items == [{"unit_price": 100, "qty": 2}]


def test_items_json_corrupted_detected():
    items, error = parse_items_json("{not valid json")
    assert items is None
    assert error == "JSON_ITEMS_CORRUPTED"


def test_items_json_empty_list_detected():
    items, error = parse_items_json("[]")
    assert items is None
    assert error == "ITEMS_EMPTY"


def test_items_json_missing_detected():
    items, error = parse_items_json(None)
    assert error == "JSON_ITEMS_CORRUPTED"


# --------------------------------------------------------------------------
# 8) إزالة BOM من أسماء المفاتيح (strip_bom_keys)
# --------------------------------------------------------------------------

def test_strip_bom_from_key_name():
    record = {"\ufefforder_id": "طلب-1", "customer_id": "ع-1"}
    cleaned = strip_bom_keys(record)
    assert "order_id" in cleaned
    assert "\ufefforder_id" not in cleaned
    assert cleaned["order_id"] == "طلب-1"