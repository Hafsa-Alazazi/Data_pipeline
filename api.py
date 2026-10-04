

import json
import logging
import os
import re
import shutil
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, Field

from config import settings

logger = logging.getLogger("api")

# تشغيل المُجدوِل في الخلفية مع إقلاع الـ API (يمكن تعطيله ENABLE_SCHEDULER=false)
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "true").strip().lower() not in ("0", "false", "no")
UPLOADS_DIR = os.path.join(settings.DATA_DIR, "uploads")


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = None
    if ENABLE_SCHEDULER:
        try:
            from src.phase2.jobs import start_background_scheduler
            scheduler = start_background_scheduler()
            logger.info("Background scheduler started.")
        except Exception as exc:  # noqa: BLE001 - الـ API يجب أن يعمل حتى لو فشل المُجدوِل
            logger.warning("Scheduler not started: %s", exc)
    app.state.scheduler = scheduler
    yield
    if scheduler is not None:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="Big Data Orders Pipeline - Final Project API",
    description="واجهة تشغيل موحدة: الإدخال، الفهارس، الاستعلامات، التجميعات، "
                "العروض المادية، والمهام المجدولة.",
    version="2.0.0",
    lifespan=lifespan,
)




def _json_safe(obj):
    """يحوّل أي نتيجة (ObjectId, datetime, SON...) إلى JSON آمن."""
    return json.loads(json.dumps(obj, default=str, ensure_ascii=False))


def _http_error(exc: Exception) -> HTTPException:
    """يحوّل أخطاء المشروع إلى رموز HTTP مناسبة بدل 500 عام."""
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc.args[0]) if exc.args else "Not found")
    if isinstance(exc, FileNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, (ValueError, TypeError)):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, ConnectionError):
        return HTTPException(status_code=503, detail=f"MongoDB unavailable: {exc}")
    return HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


def _resolve_input_path(path: str) -> str:
    """المسار النسبي يُحل أولاً من مجلد التشغيل ثم من جذر المشروع."""
    if os.path.isabs(path) or os.path.exists(path):
        return path
    candidate = os.path.join(settings.BASE_DIR, path)
    return candidate if os.path.exists(candidate) else path


def _run_ingest(path: str) -> dict:
    # استيراد متأخر: main يستورد مكونات المشروع النصفي (Spark يُحمَّل فقط عند الحاجة)
    from main import run_pipeline
    try:
        return _json_safe(run_pipeline(_resolve_input_path(path)))
    except AssertionError as exc:
        raise HTTPException(status_code=422, detail=f"Consistency check failed: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise _http_error(exc) from exc


# --------------------------------------------------------------------------
# GET /health
# --------------------------------------------------------------------------

@app.get("/health", tags=["system"], summary="فحص الحالة والاتصال بـ MongoDB")
def health():
    try:
        from src.mongo_setup import get_mongo_client
        client = get_mongo_client()
        try:
            db = client[settings.MONGO_DB_NAME]
            counts = {
                name: db[name].estimated_document_count()
                for name in (settings.COLLECTION_RAW, settings.COLLECTION_VALIDATED, settings.COLLECTION_QUARANTINE)
            }
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=503,
            detail={"status": "degraded", "mongo": "down", "error": str(exc)},
        ) from exc
    return {
        "status": "ok",
        "mongo": "up",
        "database": settings.MONGO_DB_NAME,
        "collections": counts,
        "scheduler_running": bool(getattr(app.state, "scheduler", None)),
    }


# --------------------------------------------------------------------------
# POST /ingest  (نفس بوابة الإدخال main.run_pipeline - لا مسار جديد)
# --------------------------------------------------------------------------

class IngestRequest(BaseModel):
    input_path: str = Field(..., description="مسار ملف CSV على الجهاز الذي يعمل عليه الـ API.")


@app.post("/ingest", tags=["ingest"], summary="تشغيل خط الإدخال الحالي على ملف CSV (بمساره)")
def ingest(body: IngestRequest):
    return _run_ingest(body.input_path)


@app.post("/ingest/upload", tags=["ingest"], summary="رفع ملف CSV ثم تشغيل نفس خط الإدخال عليه")
def ingest_upload(file: UploadFile = File(...)):
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    safe_name = re.sub(r"[^\w.\-]", "_", os.path.basename(file.filename or "upload.csv")) or "upload.csv"
    target = os.path.join(UPLOADS_DIR, safe_name)
    with open(target, "wb") as handle:
        shutil.copyfileobj(file.file, handle)
    return _run_ingest(target)


# --------------------------------------------------------------------------
# POST /indexes
# --------------------------------------------------------------------------

@app.post("/indexes", tags=["indexes"], summary="إنشاء فهارس المشروع النهائي (آمن للتكرار)")
def create_indexes(
    with_explain: bool = Query(
        False,
        description="إن كانت true: يُنفَّذ explain قبل/بعد الفهارس ويُحدَّث docs/explain_report.md "
                    "(يحذف فهارس المرحلة الثانية مؤقتًا ثم يعيد إنشاءها، وقد يستغرق وقتًا على بيانات ضخمة).",
    ),
):
    from src.phase2 import indexes
    try:
        response = {"created": indexes.create_phase2_indexes()}
        if with_explain:
            from src.phase2 import explain_report
            report = explain_report.write_report()
            response["explain_report_file"] = os.path.relpath(report["path"], settings.BASE_DIR)
            response["explain_report_markdown"] = report["report_markdown"]
        response["existing_indexes"] = indexes.list_existing_indexes()
        return _json_safe(response)
    except Exception as exc:  # noqa: BLE001
        raise _http_error(exc) from exc


# --------------------------------------------------------------------------
# GET /queries , GET /queries/{name}
# --------------------------------------------------------------------------

@app.get("/queries", tags=["queries"], summary="قائمة الاستعلامات المتاحة ومعاملاتها")
def list_queries():
    from src.phase2.queries import QUERY_REGISTRY
    return {
        "count": len(QUERY_REGISTRY),
        "queries": [
            {
                "name": name,
                "description": meta["description"],
                "params": meta.get("params", {}),
                "index": meta["index_used"],
                "run": f"GET /queries/{name}",
            }
            for name, meta in QUERY_REGISTRY.items()
        ],
    }


@app.get("/queries/{name}", tags=["queries"],
         summary="تنفيذ استعلام بالاسم (المعاملات كـ query string)")
def run_named_query(
    name: str,
    request: Request,
    explain: bool = Query(False, description="true = إرجاع ملخص executionStats بدل النتائج"),
    # الحقول التالية موجودة لتظهر في Swagger فقط؛ القيم تُقرأ من request.query_params
    city: Optional[str] = Query(None, description="find_orders_by_city"),
    start_date: Optional[str] = Query(None, description="find_orders_by_date_range (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="find_orders_by_date_range (YYYY-MM-DD)"),
    status: Optional[str] = Query(None, description="find_orders_by_status_and_payment"),
    payment_status: Optional[str] = Query(None, description="find_orders_by_status_and_payment"),
    customer_id: Optional[str] = Query(None, description="find_orders_by_customer"),
    limit: Optional[int] = Query(None, ge=1, description="الحد الأقصى للنتائج (اختياري)"),
):
    """name = اسم الاستعلام فقط (مثل find_top_orders_by_amount). راجع GET /queries للأسماء والمعاملات."""
    from src.phase2 import queries
    params = {k: v for k, v in request.query_params.items() if k != "explain" and v != ""}
    try:
        if explain:
            result = queries.explain_query(name, params)
            result.pop("raw_explain", None)  # الخام ضخم؛ الملخص كافٍ هنا
            return _json_safe(result)
        return _json_safe(queries.run_query(name, params))
    except Exception as exc:  # noqa: BLE001
        raise _http_error(exc) from exc


# --------------------------------------------------------------------------
# GET /aggregations , GET /aggregations/{name}
# --------------------------------------------------------------------------

@app.get("/aggregations", tags=["aggregations"], summary="قائمة تقارير التجميع المتاحة")
def list_aggregations():
    from src.phase2.aggregations import AGGREGATION_REGISTRY
    return {
        "count": len(AGGREGATION_REGISTRY),
        "reports": [
            {
                "name": name,
                "description": meta["description"],
                "params": meta.get("params", {}),
                "run": f"GET /aggregations/{name}",
            }
            for name, meta in AGGREGATION_REGISTRY.items()
        ],
    }


@app.get("/aggregations/{name}", tags=["aggregations"],
         summary="تشغيل تقرير تجميع بالاسم (المعاملات كـ query string)")
def run_named_aggregation(
    name: str,
    request: Request,
    limit: Optional[int] = Query(None, ge=1, description="اختياري: sales_by_city / top_products / top_customers"),
    granularity: Optional[str] = Query(None, description="اختياري لـ sales_by_period: month أو day"),
):
    """name = اسم التقرير فقط (مثل sales_by_city). راجع GET /aggregations للأسماء."""
    from src.phase2 import aggregations
    try:
        params = {k: v for k, v in request.query_params.items() if v != ""}
        return _json_safe(aggregations.run_aggregation(name, params))
    except Exception as exc:  # noqa: BLE001
        raise _http_error(exc) from exc


# --------------------------------------------------------------------------
# POST /refresh-mv
# --------------------------------------------------------------------------

@app.post("/refresh-mv", tags=["materialized-views"],
          summary="تحديث العرضين الماديين (تزايدي افتراضيًا)")
def refresh_mv(full: bool = Query(False, description="true = إعادة بناء كاملة بدل التحديث التزايدي")):
    from src.phase2 import materialized_views
    try:
        return _json_safe(materialized_views.refresh_all_materialized_views(full=full))
    except Exception as exc:  # noqa: BLE001
        raise _http_error(exc) from exc


# --------------------------------------------------------------------------
# GET /jobs , POST /jobs/{name}/run
# --------------------------------------------------------------------------

@app.get("/jobs", tags=["jobs"], summary="المهام المجدولة وجداولها وآخر تشغيل وسجل التشغيلات")
def list_scheduled_jobs(history_limit: int = Query(20, ge=0, le=200)):
    from src.phase2 import jobs
    try:
        return _json_safe({
            "scheduler_running": bool(getattr(app.state, "scheduler", None)),
            "jobs": jobs.list_jobs(),
            "recent_runs": jobs.get_run_history(limit=history_limit),
        })
    except Exception as exc:  # noqa: BLE001
        raise _http_error(exc) from exc


@app.post("/jobs/{name}/run", tags=["jobs"], summary="تشغيل مهمة يدويًا الآن (يُسجَّل التشغيل)")
def run_job_now(name: str):
    from src.phase2 import jobs
    try:
        # فشل المهمة نفسها لا يرفع استثناء: يُعاد سجل التشغيل بحالة failed
        return _json_safe(jobs.run_job(name, trigger="manual"))
    except Exception as exc:  # noqa: BLE001
        raise _http_error(exc) from exc
