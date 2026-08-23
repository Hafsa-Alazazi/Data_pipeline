
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.quality_rules import classify_record


def _base_record(**overrides) -> dict:
    
    record = {
        "order_id": "طلب-1",
        "order_date": "2025-01-31T00:00:00",
        "status": "confirmed",
        "customer_id": "عميل-1",
        "customer_name": "أحمد",
        "customer_phone": "+967771234567",
        "customer_email": "user@mail.com",
        "city": "صنعاء",
        "district": "الحصبة",
        "delivery_type": "توصيل",
        "delivery_cost": "500",
        "payment_method": "نقدي",
        "payment_status": "paid",
        "payment_amount": "5000",
        "currency": "YER",
        "total_amount": "5500",
        "items_json": '[{"unit_price": 5000, "qty": 1}]',
    }
    record.update(overrides)
    return record




def test_fully_clean_record_classified_as_valid():
    """سجل سليم 100% منذ البداية يجب ألا يمر بأي تصحيح، ويُصنَّف valid."""
    result = classify_record(_base_record())
    assert result["quality_status"] == "valid"
    assert result["corrections"] == []
    assert result["quarantine_code"] is None
    assert result["cleaned_record"] is not None




def test_arabic_price_triggers_correction_with_audit_trail():
    
    record = _base_record(payment_amount="٥٠٠٠")
    result = classify_record(record)

    assert result["quality_status"] == "corrected"
    assert len(result["corrections"]) >= 1

    correction = next(c for c in result["corrections"] if c["field"] == "payment_amount")
    assert correction["original_value"] == "٥٠٠٠"
    assert correction["corrected_value"] == 5000.0
    assert correction["rule_code"] == "ARABIC_DIGITS"


def test_repeated_email_symbol_triggers_correction():
    record = _base_record(customer_email="user@@mail.com")
    result = classify_record(record)

    assert result["quality_status"] == "corrected"
    correction = next(c for c in result["corrections"] if c["field"] == "customer_email")
    assert correction["corrected_value"] == "user@mail.com"
    assert correction["rule_code"] == "EMAIL_REPEATED_SYMBOLS"




def test_missing_order_id_quarantined():
    record = _base_record(order_id="")
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "ID_ORDER_MISSING"
    assert result["cleaned_record"] is None


def test_missing_customer_id_quarantined():
    record = _base_record(customer_id="")
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "ID_CUSTOMER_MISSING"


def test_corrupted_items_json_quarantined():
    record = _base_record(items_json="{not valid json")
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "JSON_ITEMS_CORRUPTED"


def test_empty_items_quarantined():
    record = _base_record(items_json="[]")
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "ITEMS_EMPTY"


def test_negative_quantity_inside_items_quarantined():
    
    record = _base_record(items_json='[{"unit_price": 100, "qty": -2}]')
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "VALUE_NEGATIVE_AMBIGUOUS"


def test_impossible_date_quarantined():
    record = _base_record(order_date="2025-13-40")
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "DATE_IMPOSSIBLE_INVALID"


def test_unfixable_email_quarantined():
    record = _base_record(customer_email="usermail.com")
    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "EMAIL_UNFIXABLE"




def test_multiple_core_errors_produce_conflicting_code():
    
    record = _base_record(
        customer_id="",
        items_json='[{"unit_price": 100, "qty": -2}]',
    )
    result = classify_record(record)

    assert result["quality_status"] == "quarantined"
    assert result["quarantine_code"] == "ERRORS_CONFLICTING_MULTIPLE"
    assert "ID_CUSTOMER_MISSING" in result["quarantine_codes"]
    assert "VALUE_NEGATIVE_AMBIGUOUS" in result["quarantine_codes"]


def test_single_core_error_does_not_use_conflicting_code():
    
    record = _base_record(customer_id="")
    result = classify_record(record)
    assert result["quarantine_code"] != "ERRORS_CONFLICTING_MULTIPLE"
    assert result["quarantine_codes"] == ["ID_CUSTOMER_MISSING"]




def test_mismatched_total_is_recomputed_from_items():
    """
    إذا كان total_amount لا يطابق (مجموع العناصر + التوصيل)، يجب إعادة
    حسابه تلقائيًا وتسجيل ذلك في Audit Trail بكود TOTAL_RECOMPUTED.
    """
    record = _base_record(
        delivery_cost="500",
        total_amount="9999",  
        items_json='[{"unit_price": 5000, "qty": 1}]',
    )
    result = classify_record(record)

    assert result["quality_status"] == "corrected"
    correction = next(c for c in result["corrections"] if c["rule_code"] == "TOTAL_RECOMPUTED")
    assert correction["corrected_value"] == 5500.0




def test_result_always_has_exactly_one_final_status():
    
    test_records = [
        _base_record(),
        _base_record(payment_amount="٥٠٠٠"),
        _base_record(order_id=""),
        _base_record(items_json='[{"unit_price": 100, "qty": -2}]'),
    ]
    for record in test_records:
        result = classify_record(record)
        assert result["quality_status"] in ("valid", "corrected", "quarantined")