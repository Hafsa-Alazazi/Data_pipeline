

import json
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def build_metrics_record(router_ctx: dict, load_summary: dict, elt_summary: dict) -> dict:
   
    file_name = os.path.basename(router_ctx["file_path"])

    throughput = load_summary.get("throughput_rows_per_sec")
    if throughput is None and load_summary.get("seconds_elapsed"):
        loaded = load_summary.get("loaded_raw", 0)
        throughput = round(loaded / load_summary["seconds_elapsed"], 2) if load_summary["seconds_elapsed"] > 0 else 0

    record = {
        "id_run": router_ctx["id_run"],
        "recorded_at": datetime.now(timezone.utc).isoformat(),

       
        "file_name": file_name,
        "file_size_mb": router_ctx["file_size_mb"],
        "used_engine": router_ctx["engine"],
        "engine_selection_reason": router_ctx["reason"],

       
        "read_rows": load_summary.get("read_rows"),
        "loaded_raw": load_summary.get("loaded_raw"),
        "load_seconds_elapsed": load_summary.get("seconds_elapsed"),
        "throughput_rows_per_sec": throughput,
        "batch_size_setting": load_summary.get("batch_size_setting"),
        "partitions_input": load_summary.get("partitions_input"),

        
        "elt_seconds_elapsed": elt_summary.get("seconds_elapsed"),
        "count_valid": elt_summary.get("count_valid"),
        "count_corrected": elt_summary.get("count_corrected"),
        "count_quarantine": elt_summary.get("count_quarantine"),
        "counts_case_error": elt_summary.get("counts_case_error"),
        "consistency_check_passed": elt_summary.get("consistency_check_passed"),

       
        "count_inserted": elt_summary.get("count_inserted"),
        "count_updated": elt_summary.get("count_updated"),
        "count_unchanged": elt_summary.get("count_unchanged"),

        
        "total_seconds_elapsed": round(
            (load_summary.get("seconds_elapsed") or 0) + (elt_summary.get("seconds_elapsed") or 0), 2
        ),
    }
    return record


def save_metrics(record: dict) -> str:
    
    os.makedirs(settings.REPORTS_DIR, exist_ok=True)

    existing_runs = []
    if os.path.exists(settings.RESULTS_JSON_PATH):
        try:
            with open(settings.RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
                existing_runs = json.load(f)
            if not isinstance(existing_runs, list):
                existing_runs = [existing_runs]
        except (json.JSONDecodeError, OSError):
           
            existing_runs = []

    existing_runs.append(record)

    with open(settings.RESULTS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(existing_runs, f, ensure_ascii=False, indent=2, default=str)

    return settings.RESULTS_JSON_PATH


def print_metrics_summary(record: dict):
    
    print("=" * 60)
    print("[metrics] ملخص المقاييس النهائي للتشغيل")
    print("=" * 60)
    for key, value in record.items():
        print(f"    {key}: {value}")
    print("=" * 60)


if __name__ == "__main__":
    
    if os.path.exists(settings.RESULTS_JSON_PATH):
        with open(settings.RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
            runs = json.load(f)
        print(f"[metrics] عدد التشغيلات المسجّلة حتى الآن: {len(runs)}")
        if runs:
            print_metrics_summary(runs[-1])
    else:
        print(f"[metrics] لا يوجد ملف نتائج بعد في {settings.RESULTS_JSON_PATH}")