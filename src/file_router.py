

import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def generate_run_id() -> str:
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    return f"run_{timestamp}_{short_uuid}"


def get_file_size_mb(file_path: str) -> float:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 * 1024)


def choose_engine(file_size_mb: float) -> str:
    
    if file_size_mb <= settings.SMALL_FILE_THRESHOLD_MB:
        return "python_batch"
    return "pyspark"


def route_file(file_path: str) -> dict:
    
    file_size_mb = get_file_size_mb(file_path)
    engine = choose_engine(file_size_mb)
    id_run = generate_run_id()

    reason = (
        f"حجم الملف ({file_size_mb:.2f} MB) "
        f"{'أقل من أو يساوي' if engine == 'python_batch' else 'أكبر من'} "
        f"الحد الفاصل ({settings.SMALL_FILE_THRESHOLD_MB} MB)"
    )

    print("=" * 60)
    print("[file_router] نتيجة التوجيه (Routing Decision)")
    print(f"    id_run         : {id_run}")
    print(f"    file_path      : {file_path}")
    print(f"    file_size_mb   : {file_size_mb:.2f} MB")
    print(f"    threshold_mb   : {settings.SMALL_FILE_THRESHOLD_MB} MB")
    print(f"    engine_chosen  : {engine}")
    print(f"    reason         : {reason}")
    print("=" * 60)

    return {
        "id_run": id_run,
        "file_path": file_path,
        "file_size_mb": round(file_size_mb, 2),
        "threshold_mb": settings.SMALL_FILE_THRESHOLD_MB,
        "engine": engine,
        "reason": reason,
    }


if __name__ == "__main__":
    
    import argparse

    parser = argparse.ArgumentParser(description="اختبار مستقل لـ File Router")
    parser.add_argument("--input", required=True, help="مسار الملف المراد فحصه")
    args = parser.parse_args()

    route_file(args.input)