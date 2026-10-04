"""
src/phase2/jobs.py
------------------
المهام المجدولة (Scheduled Jobs) - المتطلب 4 من المشروع النهائي.

مهمتان فعليتان مرتبطتان بالمشروع:
  1) refresh_materialized_views : تحديث تزايدي لـ daily_sales_summary و top_products_summary
  2) periodic_report            : توليد تقرير دوري من تقارير الـ Aggregations الخمسة
                                  (يُحفظ في MongoDB ويُصدَّر كملف JSON)

كل تشغيل (مجدول أو يدوي) يُسجَّل في مجموعة phase2_job_runs:
  اسم المهمة، نوع التشغيل (scheduled / manual)، وقت البداية، وقت النهاية،
  المدة، الحالة (success / failed)، النتيجة أو رسالة الخطأ.

الجدول الزمني قابل للضبط بمتغيرات بيئة (القيم الافتراضية في .env.example):
  JOB_REFRESH_MV_EVERY_MINUTES  (افتراضي 15)  : تكرار تحديث الـ Views بالدقائق
  JOB_REPORT_HOUR / JOB_REPORT_MINUTE (2 / 0) : وقت التقرير الدوري يوميًا
  JOBS_TIMEZONE                  (افتراضي UTC)

التشغيل اليدوي (للاختبار والمناقشة):
    py -m src.phase2.jobs list              # عرض المهام وجدولها وآخر تشغيل
    py -m src.phase2.jobs run <job_name>    # تشغيل مهمة الآن
    py -m src.phase2.jobs history           # سجل التشغيلات
    py -m src.phase2.jobs serve             # تشغيل المُجدوِل (يبقى يعمل حتى Ctrl+C)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
from src.mongo_setup import get_mongo_client
from src.phase2 import aggregations, materialized_views

JOB_RUNS_COLLECTION = "phase2_job_runs"
PERIODIC_REPORTS_COLLECTION = "phase2_periodic_reports"
PERIODIC_REPORTS_SUBDIR = "periodic_reports"


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


REFRESH_EVERY_MINUTES = _env_int("JOB_REFRESH_MV_EVERY_MINUTES", 15, 1, 24 * 60)
REPORT_HOUR = _env_int("JOB_REPORT_HOUR", 2, 0, 23)
REPORT_MINUTE = _env_int("JOB_REPORT_MINUTE", 0, 0, 59)
SCHEDULER_TIMEZONE = os.getenv("JOBS_TIMEZONE", "UTC")


# --------------------------------------------------------------------------
# منطق المهمتين (كل دالة ترجع dict صغيرًا قابلًا للتحويل إلى JSON)
# --------------------------------------------------------------------------

def _job_refresh_materialized_views(db) -> dict:
    summary = materialized_views.refresh_all_materialized_views()
    return {
        "views": [
            {k: v for k, v in item.items() if k not in ("touched_days", "touched_skus")}
            for item in summary["views"]
        ]
    }


def _job_periodic_report(db) -> dict:
    generated_at = datetime.now(timezone.utc)
    reports = {}
    for name in aggregations.AGGREGATION_REGISTRY:
        output = aggregations.run_aggregation(name)
        reports[name] = {
            "description": output["description"],
            "result_count": output["result_count"],
            "results": output["results"],
        }

    snapshot = {"generated_at": generated_at, "reports": reports}
    report_id = db[PERIODIC_REPORTS_COLLECTION].insert_one(snapshot).inserted_id

    # تصدير نسخة ملف JSON بجانب التقارير الأخرى
    export_dir = os.path.join(settings.REPORTS_DIR, PERIODIC_REPORTS_SUBDIR)
    os.makedirs(export_dir, exist_ok=True)
    file_name = f"periodic_report_{generated_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    file_path = os.path.join(export_dir, file_name)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"generated_at": generated_at.isoformat(), "reports": reports},
            handle, ensure_ascii=False, indent=2, default=str,
        )

    return {
        "report_id": str(report_id),
        "exported_file": os.path.relpath(file_path, settings.BASE_DIR),
        "result_counts": {name: data["result_count"] for name, data in reports.items()},
    }


JOBS = {
    "refresh_materialized_views": {
        "description": "تحديث تزايدي للعروض المادية (daily_sales_summary و top_products_summary).",
        "func": _job_refresh_materialized_views,
        "schedule_text": f"كل {REFRESH_EVERY_MINUTES} دقيقة",
        "trigger_args": {"trigger": "interval", "minutes": REFRESH_EVERY_MINUTES},
    },
    "periodic_report": {
        "description": "توليد تقرير دوري من تقارير الـ Aggregations الخمسة وحفظه في MongoDB وكملف JSON.",
        "func": _job_periodic_report,
        "schedule_text": f"يوميًا الساعة {REPORT_HOUR:02d}:{REPORT_MINUTE:02d} ({SCHEDULER_TIMEZONE})",
        "trigger_args": {"trigger": "cron", "hour": REPORT_HOUR, "minute": REPORT_MINUTE},
    },
}


# --------------------------------------------------------------------------
# التشغيل والتسجيل
# --------------------------------------------------------------------------

def _iso(value):
    """تاريخ بصيغة ISO موحّدة UTC (MongoDB يُرجع التواريخ بدون منطقة زمنية وبدقة ميلي ثانية)."""
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _public_run(doc: dict) -> dict:
    """سجل تشغيل جاهز لـ JSON."""
    return {
        "run_id": str(doc.get("_id")),
        "job": doc.get("job"),
        "trigger": doc.get("trigger"),
        "started_at": _iso(doc.get("started_at")),
        "finished_at": _iso(doc.get("finished_at")),
        "duration_seconds": doc.get("duration_seconds"),
        "status": doc.get("status"),
        "result": doc.get("result"),
        "error": doc.get("error"),
    }


def _open_db(db):
    if db is not None:
        return None, db
    client = get_mongo_client()
    return client, client[settings.MONGO_DB_NAME]


def run_job(name: str, trigger: str = "manual", db=None) -> dict:
    """
    يشغّل مهمة بالاسم ويسجّل بدايتها ونهايتها وحالتها.
    فشل المهمة لا يرفع استثناءً: يُسجَّل في السجل بحالة failed ويُعاد في الرد.
    """
    if name not in JOBS:
        raise KeyError(f"مهمة غير معروفة: {name}. المتاح: {list(JOBS)}")

    client, db = _open_db(db)
    try:
        runs = db[JOB_RUNS_COLLECTION]
        started_at = datetime.now(timezone.utc)
        clock = time.perf_counter()
        run_id = runs.insert_one({
            "job": name, "trigger": trigger, "started_at": started_at,
            "finished_at": None, "status": "running",
        }).inserted_id

        status, result, error = "success", None, None
        try:
            result = JOBS[name]["func"](db)
        except Exception as exc:  # noqa: BLE001 - نسجّل أي فشل بدل إسقاط المُجدوِل
            status, error = "failed", f"{type(exc).__name__}: {exc}"

        update = {
            "finished_at": datetime.now(timezone.utc),
            "duration_seconds": round(time.perf_counter() - clock, 3),
            "status": status, "result": result, "error": error,
        }
        runs.update_one({"_id": run_id}, {"$set": update})
        return _public_run({"_id": run_id, "job": name, "trigger": trigger,
                            "started_at": started_at, **update})
    finally:
        if client is not None:
            client.close()


def list_jobs(db=None) -> list:
    """المهام المتاحة مع جدولها وآخر تشغيل لكل منها."""
    client, db = _open_db(db)
    try:
        listing = []
        for name, meta in JOBS.items():
            last = db[JOB_RUNS_COLLECTION].find_one({"job": name}, sort=[("started_at", -1)])
            listing.append({
                "name": name,
                "description": meta["description"],
                "schedule": meta["schedule_text"],
                "last_run": _public_run(last) if last else None,
            })
        return listing
    finally:
        if client is not None:
            client.close()


def get_run_history(job: str | None = None, limit: int = 20, db=None) -> list:
    client, db = _open_db(db)
    try:
        query = {"job": job} if job else {}
        cursor = db[JOB_RUNS_COLLECTION].find(query).sort("started_at", -1).limit(int(limit))
        return [_public_run(doc) for doc in cursor]
    finally:
        if client is not None:
            client.close()


# --------------------------------------------------------------------------
# المُجدوِل (APScheduler)
# --------------------------------------------------------------------------

def _scheduled_entry(name: str):
    run_job(name, trigger="scheduled")


def build_scheduler(blocking: bool = False):
    """ينشئ مُجدوِلًا مضبوطًا بمهام المشروع (لم يبدأ بعد). apscheduler يُستورد هنا فقط."""
    if blocking:
        from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler
    else:
        from apscheduler.schedulers.background import BackgroundScheduler as Scheduler

    scheduler = Scheduler(timezone=SCHEDULER_TIMEZONE)
    for name, meta in JOBS.items():
        scheduler.add_job(
            _scheduled_entry, args=[name], id=name, name=name,
            coalesce=True, max_instances=1, misfire_grace_time=300,
            replace_existing=True, **meta["trigger_args"],
        )
    return scheduler


def start_background_scheduler():
    """تستخدمه واجهة الـ API عند الإقلاع لتبقى المهام تعمل في الخلفية."""
    scheduler = build_scheduler(blocking=False)
    scheduler.start()
    return scheduler


def _print_run(run: dict):
    print(f"[{run['status']}] {run['job']}  ({run['trigger']})  "
          f"{run['started_at']} -> {run['finished_at']}  ({run['duration_seconds']}s)")
    if run["error"]:
        print(f"    خطأ: {run['error']}")
    elif run["result"] is not None:
        print(f"    النتيجة: {json.dumps(run['result'], ensure_ascii=False, default=str)}")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "list"

    if command == "list":
        for job in list_jobs():
            print(f"- {job['name']}: {job['description']}")
            print(f"    الجدول: {job['schedule']}")
            print(f"    آخر تشغيل: {job['last_run']['status'] + ' @ ' + job['last_run']['started_at'] if job['last_run'] else 'لم يُشغَّل بعد'}")
    elif command == "run":
        if len(sys.argv) < 3:
            sys.exit(f"الاستخدام: py -m src.phase2.jobs run <job_name>  (المتاح: {list(JOBS)})")
        _print_run(run_job(sys.argv[2], trigger="manual"))
    elif command == "history":
        for entry in get_run_history():
            _print_run(entry)
    elif command == "serve":
        print("[jobs] تشغيل المُجدوِل... (Ctrl+C للإيقاف)")
        for job in JOBS.values():
            print(f"    - {job['schedule_text']}")
        try:
            build_scheduler(blocking=True).start()
        except (KeyboardInterrupt, SystemExit):
            print("[jobs] تم إيقاف المُجدوِل.")
    else:
        sys.exit("الأوامر المتاحة: list | run <job_name> | history | serve")