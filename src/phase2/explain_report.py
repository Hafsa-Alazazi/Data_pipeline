"""
src/phase2/explain_report.py
-------------------------------
ينفّذ executionStats على 3 استعلامات من QUERY_REGISTRY مرتين: مرة قبل
إنشاء فهارس المشروع النهائي (بعد حذفها إن وُجدت)، ومرة بعدها - ويكتب
تقرير مقارنة فعلي (أرقام حقيقية من قاعدة البيانات الحالية وقت التشغيل،
وليست أرقامًا مثبتة بالكود) إلى docs/explain_report.md.

يشرح التقرير أيضًا لماذا اختير كل فهرس (من INDEX_DEFINITIONS) وأثره المحسوب
فعليًا (عدد المستندات المفحوصة والزمن قبل/بعد).

الاستخدام:
    py -m src.phase2.explain_report

مبدأ مهم: هذا السكربت لا يفترض عدد سجلات معيّن أو نتيجة معيّنة - يقرأ
فقط ما هو موجود فعليًا في orders_validated وقت التشغيل، ويعمل بنفس
الطريقة أيًا كانت البيانات الفعلية (تدريب أو اختبار).
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
from src.mongo_setup import get_mongo_client
from src.phase2.indexes import INDEX_DEFINITIONS, create_phase2_indexes
from src.phase2.queries import explain_query


# الاستعلامات الثلاثة المطلوبة صراحة للمقارنة (القسم 1: "3 استعلامات قبل وبعد").
# أسماء الحقول فقط ثابتة هنا (بنية البيانات)؛ القيم الفعلية (status,
# payment_status, customer_id) تُسحب ديناميكيًا من البيانات الحقيقية
# الموجودة وقت التشغيل عبر _pick_sample_values() أدناه - لا قيم مثبّتة.
EXPLAIN_SAMPLE_QUERIES = [
    "find_orders_by_status_and_payment",
    "find_top_orders_by_amount",
    "find_orders_by_customer",
]


def _pick_sample_values() -> dict:
    """
    يسحب قيمًا فعلية من البيانات الحية وقت التشغيل (لا قيم ثابتة بالكود):
      - customer_id من أول وثيقة.
      - (status, payment_status): أندر توليفة موجودة فعليًا. اختيار توليفة نادرة
        مقصود: لو اخترنا توليفة شائعة لأوقف LIMIT الفحص مبكرًا حتى بدون فهرس
        فلا يظهر أثر الفهرس في المقارنة.
    """
    client = get_mongo_client()
    try:
        db = client[settings.MONGO_DB_NAME]
        collection = db[settings.COLLECTION_VALIDATED]
        doc = collection.find_one({}, {"customer_id": 1, "status": 1, "payment_status": 1}) or {}
        try:
            rare = list(collection.aggregate([
                {"$match": {
                    "status": {"$type": "string", "$ne": ""},
                    "payment_status": {"$type": "string", "$ne": ""},
                }},
                {"$group": {"_id": {"status": "$status", "payment_status": "$payment_status"},
                            "n": {"$sum": 1}}},
                {"$sort": {"n": 1}},
                {"$limit": 1},
            ], allowDiskUse=True))
            if rare:
                doc["status"] = rare[0]["_id"]["status"]
                doc["payment_status"] = rare[0]["_id"]["payment_status"]
        except Exception as exc:  # noqa: BLE001 - نرجع للقيم الافتراضية من أول وثيقة
            print(f"[explain_report] تعذّر اختيار توليفة نادرة، سيُستخدم أول سجل: {exc}")
    finally:
        client.close()
    return doc


def _drop_phase2_indexes_if_exist():
    """
    يحذف الفهارس التي تغطي نفس حقول فهارس المشروع النهائي (أيًا كان اسمها) لقياس
    حالة 'قبل' بدقة؛ وإلا لاستخدم الاستعلام فهرسًا قديمًا بإسم آخر وظهر 'قبل' محسّنًا.
    """
    wanted_keys = {tuple(index_def["keys"]) for index_def in INDEX_DEFINITIONS}
    client = get_mongo_client()
    try:
        db = client[settings.MONGO_DB_NAME]
        collection = db[settings.COLLECTION_VALIDATED]
        for name, info in list(collection.index_information().items()):
            if name != "_id_" and tuple(info["key"]) in wanted_keys:
                collection.drop_index(name)
    finally:
        client.close()


def _run_all_sample_queries(sample_values: dict) -> list:
    results = []
    for name in EXPLAIN_SAMPLE_QUERIES:
        if name == "find_orders_by_status_and_payment":
            params = {
                "status": sample_values.get("status"),
                "payment_status": sample_values.get("payment_status"),
                "limit": 50,
            }
        elif name == "find_top_orders_by_amount":
            params = {"limit": 10}
        elif name == "find_orders_by_customer":
            params = {"customer_id": sample_values.get("customer_id"), "limit": 50}
        else:
            params = {}
        try:
            # تشغيل تسخيني أول (يُهمَل) ثم قياس فعلي: الأول يتأثر بتحميل الصفحات من القرص
            # وببناء الفهرس للتو، فيعطي زمنًا مضللًا (قد يبدو الفهرس أبطأ).
            explain_query(name, params)
            results.append(explain_query(name, params))
        except Exception as exc:
            results.append({"query_name": name, "error": str(exc)})
    return results


def _plan_summary(explain_result: dict) -> tuple:
    """
    يمشي على شجرة خطة التنفيذ الفائزة ويعيد (سلسلة المراحل، أسماء الفهارس المستخدمة).
    أعلى مرحلة وحدها (مثل LIMIT) لا تكفي: المهم هو COLLSCAN أم IXSCAN وأي فهرس.
    """
    raw = explain_result.get("raw_explain") or {}
    plan = (raw.get("queryPlanner") or {}).get("winningPlan") or {}
    plan = plan.get("queryPlan", plan)  # محرك SBE يضع الخطة التقليدية داخل queryPlan

    stages, indexes = [], []

    def walk(node):
        if not isinstance(node, dict):
            return
        if node.get("stage"):
            stages.append(node["stage"])
        if node.get("indexName"):
            indexes.append(node["indexName"])
        if "inputStage" in node:
            walk(node["inputStage"])
        for child in node.get("inputStages", []) or []:
            walk(child)

    walk(plan)
    return (" > ".join(stages) or "n/a"), (", ".join(indexes) or "none")


def _impact_lines(before: dict, after: dict) -> list:
    """أثر الفهرس محسوبًا من الأرقام الفعلية (لا نصوص جاهزة)."""
    lines = []
    docs_b, docs_a = before.get("total_docs_examined"), after.get("total_docs_examined")
    if isinstance(docs_b, int) and isinstance(docs_a, int):
        text = f"Documents examined: {docs_b:,} -> {docs_a:,}"
        if docs_a > 0 and docs_b > docs_a:
            text += f" (about {docs_b / docs_a:,.0f}x fewer)"
        elif docs_a == 0 and docs_b > 0:
            text += " (no documents fetched)"
        lines.append(f"- {text}")
    time_b, time_a = before.get("execution_time_ms"), after.get("execution_time_ms")
    if time_b is not None and time_a is not None:
        lines.append(f"- Execution time: {time_b} ms -> {time_a} ms")
    return lines


def _definition_for(query_name: str):
    for index_def in INDEX_DEFINITIONS:
        if index_def["supports_query"] == query_name:
            return index_def
    return None


def generate_report() -> str:
    sample_values = _pick_sample_values()

    print("[explain_report] حذف فهارس المشروع النهائي (إن وُجدت) لقياس حالة 'قبل' بدقة...")
    _drop_phase2_indexes_if_exist()

    print("[explain_report] تنفيذ الاستعلامات الثلاثة قبل إنشاء الفهارس...")
    before = _run_all_sample_queries(sample_values)

    print("[explain_report] إنشاء فهارس المشروع النهائي...")
    create_phase2_indexes()

    print("[explain_report] تنفيذ نفس الاستعلامات الثلاثة بعد إنشاء الفهارس...")
    after = _run_all_sample_queries(sample_values)

    lines = [
        "# Explain Report: Before vs After Indexes",
        "",
        "Generated automatically by `src/phase2/explain_report.py` against the "
        "live database at run time. All numbers below are real `executionStats` "
        "values from the actual data present when this report was generated - "
        "none are hardcoded. Each query is executed once as a warm-up and "
        "measured on the second run, so execution times are not skewed by "
        "cold disk/cache effects; documents examined is the most reliable metric.",
        "",
        "## Indexes and why each one was chosen",
        "",
        "| Index | Keys | Compound | Serves query | Why |",
        "|---|---|---|---|---|",
    ]
    for index_def in INDEX_DEFINITIONS:
        keys = ", ".join(f"{field} ({'asc' if direction == 1 else 'desc'})" for field, direction in index_def["keys"])
        compound = "yes" if len(index_def["keys"]) > 1 else "no"
        reason = index_def["reason"].replace("|", "/")
        lines.append(f"| `{index_def['name']}` | {keys} | {compound} | `{index_def['supports_query']}` | {reason} |")
    lines.append("")

    for b, a in zip(before, after):
        name = b.get("query_name", "unknown")
        lines.append(f"## Query: `{name}`")
        if "error" in b or "error" in a:
            lines.append(f"- Skipped: {b.get('error') or a.get('error')}")
            lines.append("")
            continue

        index_def = _definition_for(name)
        lines.append(f"- Index expected to be used: `{b.get('index_expected')}`")
        if index_def:
            lines.append(f"- Why this index: {index_def['reason']}")
        lines.append("")

        plan_b, used_b = _plan_summary(b)
        plan_a, used_a = _plan_summary(a)
        lines.append("| Metric | Before Index | After Index |")
        lines.append("|---|---|---|")
        lines.append(f"| Plan stages | {plan_b} | {plan_a} |")
        lines.append(f"| Index used | {used_b} | {used_a} |")
        lines.append(f"| Total docs examined | {b.get('total_docs_examined')} | {a.get('total_docs_examined')} |")
        lines.append(f"| Total keys examined | {b.get('total_keys_examined')} | {a.get('total_keys_examined')} |")
        lines.append(f"| Documents returned | {b.get('n_returned')} | {a.get('n_returned')} |")
        lines.append(f"| Execution time (ms) | {b.get('execution_time_ms')} | {a.get('execution_time_ms')} |")
        lines.append("")
        lines.append("**Impact**")
        lines.extend(_impact_lines(b, a))
        lines.append("")

    return "\n".join(lines)


def write_report() -> dict:
    """يولّد التقرير ويحفظه في docs/explain_report.md ويعيد المسار والنص."""
    report = generate_report()
    docs_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "docs",
    )
    os.makedirs(docs_dir, exist_ok=True)
    output_path = os.path.join(docs_dir, "explain_report.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)
    return {"path": output_path, "report_markdown": report}


if __name__ == "__main__":
    result = write_report()
    print(f"\n[explain_report] ✅ تم حفظ التقرير في: {result['path']}")