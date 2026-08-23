"""
src/spark_loader.py
---------------------
محرك التحميل الموازي (Parallel Loading) باستخدام Apache Spark، يُستخدم
فقط عندما يختار File Router هذا المحرك (ملفات > SMALL_FILE_THRESHOLD_MB).

المبادئ الملزمة حسب القسم 6.4:
    - استخدام SparkSession وDataFrame API، وليس Pandas.
    - استخدام Schema ثابتة بدل inferSchema (inferSchema يمر على الملف
      مرتين: مرة لتخمين الأنواع ومرة للقراءة الفعلية، وهذا مكلف على
      ملف 12GB، كما أن تخمين الأنواع قد "يُفسد" القيم القذرة قبل أن
      تصل لمرحلة التنظيف - مثلاً يحوّل "٥٠٠٠" أو "706000٫0" إلى null
      تلقائيًا بدل تركها كنص ليعالجها quality_rules.py لاحقًا).
    - قراءة الحقول الحساسة كـ String في Raw للحفاظ على القيم غير النظيفة
      (لذلك كل أعمدة الـ Schema هنا StringType بدون استثناء).
    - الكتابة إلى MongoDB بالتوازي عبر MongoDB Spark Connector.
    - عدم استخدام repartition دون تبرير (نحن لا نستخدمه إطلاقًا هنا:
      قراءة CSV تُقسَّم تلقائيًا حسب عدد الـ blocks/الأنوية المتاحة،
      وإجبار repartition دون قياس فائدة فعلية سيضيف Shuffle غير مبرر
      يُعاقب عليه صراحة في القسم 6.4).
    - تسجيل عدد Input Partitions، زمن التنفيذ، ومعدل السجلات/الثانية.

مبدأ ELT (القسم 6.5): كل سجل يُكتب إلى orders_raw كما وصل تمامًا
(record_raw)، دون أي تحويل أو تصحيح - التنظيف يحدث لاحقًا في elt_pipeline.py
تمامًا كما في batch_loader.py (نفس شكل الوثيقة، نفس الحقول الستة).
"""

import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# --------------------------------------------------------------------------
# Schema ثابتة: كل الأعمدة كنص (String) عمدًا، لتُحفَظ القيم القذرة كما هي
# في orders_raw دون أي فقدان أو تحويل مبكر (القسم 6.4 و 6.5).
# الأسماء مطابقة حرفيًا لأعمدة ملف CSV الفعلي.
# --------------------------------------------------------------------------
ORDERS_CSV_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("delivery_type", StringType(), True),
    StructField("delivery_cost", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("items_json", StringType(), True),
])

RAW_SOURCE_COLUMNS = [f.name for f in ORDERS_CSV_SCHEMA.fields]


def _ensure_hadoop_home_on_windows():
    """
    إصلاح مشكلة Windows الشهيرة: Hadoop (المُستخدَم داخليًا بواسطة Spark
    حتى للملفات المحلية البسيطة، وليس فقط HDFS) يفشل بدون HADOOP_HOME
    يشير لمجلد فيه bin/winutils.exe، حتى في وضع local[*] تمامًا.

    بدل الاعتماد على أن الشخص ضبط متغير بيئة النظام يدويًا بشكل صحيح
    (عرضة للخطأ ولنسيانها وقت العرض المباشر أمام الدكتور - القسم 10)،
    نضبطها هنا برمجيًا من داخل الكود نفسه إن لم تكن مضبوطة مسبقًا،
    بشرط أن المجلد C:\\hadoop\\bin موجود فعلًا (جهّزه الطالب مسبقًا حسب
    تعليمات التثبيت في README). هذا يجعل التشغيل يعمل "من نفسه" في أي
    جلسة طرفية جديدة دون خطوات يدوية إضافية في كل مرة.
    """
    if os.name != "nt":
        return  # هذا الإصلاح خاص بـ Windows فقط؛ لا أثر له على Linux/Mac

    if os.environ.get("HADOOP_HOME"):
        return  # الشخص ضبطها مسبقًا (مثلاً على العنقود بالمسار A) - لا نتدخل

    candidate = settings.HADOOP_HOME_WINDOWS_FALLBACK
    winutils_path = os.path.join(candidate, "bin", "winutils.exe")
    if os.path.isdir(candidate) and os.path.isfile(winutils_path):
        os.environ["HADOOP_HOME"] = candidate
        print(f"[spark_loader] تم ضبط HADOOP_HOME تلقائيًا إلى: {candidate}")
    else:
        print(
            f"[spark_loader] ⚠️ تحذير: لم يُعثر على {winutils_path}. "
            "قد يفشل تشغيل Spark على Windows بخطأ HADOOP_HOME. "
            "راجع تعليمات تثبيت winutils.exe في README.md."
        )


def build_spark_session() -> SparkSession:
    """
    ينشئ SparkSession واحدة للتطبيق كاملاً.

    settings.SPARK_MASTER الافتراضي هو "local[*]" (تشغيل فردي على كل
    الأنوية المتاحة محليًا). للمسار المتقدم A (عنقود مستقل)، يُغيَّر عبر
    متغير البيئة SPARK_MASTER إلى spark://IP_MASTER:7077 دون أي تعديل
    على كود هذا الملف - هذا بالضبط سبب وضع القيمة في config/settings.py
    بدل كتابتها مباشرة هنا (القسم 9: كل الإعدادات في Config).

    ملاحظة Mongo Connector: يجب تفعيل الحزمة عبر spark.jars.packages أو
    --packages عند التشغيل. الإصدار هنا (10.4.0 لـ Scala 2.13) يجب
    التحقق من توافقه مع إصدار Spark/Scala المثبت فعليًا على الجهاز/العنقود
    (PySpark 4.x يُبنى عادة مع Scala 2.13).
    """
    _ensure_hadoop_home_on_windows()

    spark = (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config("spark.mongodb.write.connection.uri", settings.MONGO_URI)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # نقلل الضجيج، نبقي رسائلنا نحن واضحة
    return spark


def load_spark(file_path: str, run_context: dict) -> dict:
    """
    يقرأ ملف CSV الكبير عبر PySpark بالـ Schema الثابتة أعلاه، يبني وثيقة
    orders_raw لكل صف (نفس الحقول الستة المطلوبة في القسم 6.5)، ويكتبها
    بالتوازي إلى MongoDB عبر MongoDB Spark Connector.

    يعيد ملخص مقاييس بنفس شكل batch_loader.load_batches تقريبًا (لتوحيد
    الشكل في metrics.py)، مع إضافة partitions_input الخاصة بـ Spark.
    """
    spark = build_spark_session()
    start_time = time.time()

    df = (
        spark.read
        .option("header", True)
        .option("escape", '"')
        .schema(ORDERS_CSV_SCHEMA)   # Schema ثابتة، بدون inferSchema
        .csv(file_path)
        # ملاحظة تصحيح مهمة: النسخة الأولى من هذا الملف كانت تستخدم
        # .option("multiLine", True) بافتراض أن items_json قد يحتوي أسطرًا
        # حقيقية داخل حقول مقتبسة. تحقّقنا فعليًا بعدّ الأسطر عبر Python
        # (splitlines) مقابل عدّ الصفوف عبر csv.reader على نفس الملف،
        # وتطابق الرقمان تمامًا (100001 = 100001) - أي لا توجد أي أسطر
        # حقيقية داخل الحقول. multiLine=True كان يجبر Spark على قراءة
        # الملف كوحدة واحدة غير قابلة للتجزئة (Non-Splittable)، فينتج عنه
        # partition واحد فقط بغض النظر عن حجم الملف - أي معالجة تسلسلية
        # فعليًا رغم استخدام Spark بالاسم! (بالضبط حالة الخصم المذكورة في
        # القسم 13: "استخدام Spark بالاسم فقط..."). إزالته تعيد التوازي
        # الحقيقي (Spark يقسّم الملف تلقائيًا حسب spark.sql.files.maxPartitionBytes).
    )

    # عدد الـ Partitions كما قرأها Spark تلقائيًا، قبل أي تعديل - نسجّله
    # كما هو دون استخدام repartition (القسم 6.4).
    partitions_input = df.rdd.getNumPartitions()

    read_rows = df.count()  # Action أولى: تُفعّل القراءة الفعلية وتُحسب السجلات

    # number_row_source: رقم صف تقريبي (ترتيب توزيعي عبر الـ partitions،
    # وليس بالضرورة نفس ترتيب السطر الفعلي في الملف الأصلي - طبيعة أي
    # نظام موزّع). الوثيقة تطلبه "إن أمكن" (القسم 6.5)، وهذا أفضل تقريب
    # ممكن دون فرض ترتيب كلي (Global Order) يتطلب Shuffle مكلف وغير مبرر.
    df_with_meta = (
        df
        .withColumn("number_row_source", F.monotonically_increasing_id())
        .withColumn("id_run", F.lit(run_context["id_run"]))
        .withColumn("file_source", F.lit(run_context["file_path"]))
        .withColumn("at_ingested", F.current_timestamp())
        .withColumn("engine_used", F.lit("pyspark"))
        .withColumn("record_raw", F.struct(*[F.col(c) for c in RAW_SOURCE_COLUMNS]))
        .select(
            "id_run", "file_source", "number_row_source",
            "at_ingested", "engine_used", "record_raw",
        )
    )

    print(f"[spark_loader] partitions_input (كما قرأها Spark تلقائيًا): {partitions_input}")
    print(f"[spark_loader] read_rows: {read_rows:,}")
    print(f"[spark_loader] بدء الكتابة المتوازية إلى MongoDB (orders_raw)...")

    write_start = time.time()
    (
        df_with_meta.write
        .format("mongodb")
        .mode("append")
        .option("database", settings.MONGO_DB_NAME)
        .option("collection", settings.COLLECTION_RAW)
        .save()
    )
    write_elapsed = time.time() - write_start

    elapsed = time.time() - start_time
    throughput = round(read_rows / elapsed, 2) if elapsed > 0 else 0

    summary = {
        "id_run": run_context["id_run"],
        "engine_used": "pyspark",
        "read_rows": read_rows,
        "loaded_raw": read_rows,  # append فقط، بدون فلترة جودة في هذه المرحلة (ELT)
        "seconds_elapsed": round(elapsed, 2),
        "write_seconds_elapsed": round(write_elapsed, 2),
        "throughput_rows_per_sec": throughput,
        "partitions_input": partitions_input,
        "spark_master": settings.SPARK_MASTER,
    }

    print("-" * 60)
    print("[spark_loader] ملخص التحميل النهائي:")
    for key, value in summary.items():
        print(f"    {key}: {value}")
    print("-" * 60)

    spark.stop()
    return summary


if __name__ == "__main__":
    # اختبار مستقل لمحرك Spark وحده (دون المرور عبر main.py الكامل)
    import argparse
    from src.file_router import route_file

    parser = argparse.ArgumentParser(description="اختبار مستقل لمحرك PySpark Loader")
    parser.add_argument("--input", required=True, help="مسار ملف CSV كبير")
    args = parser.parse_args()

    ctx = route_file(args.input)
    if ctx["engine"] != "pyspark":
        print(
            f"⚠️ تحذير: الملف حجمه {ctx['file_size_mb']}MB وهو أصغر من الحد "
            f"({ctx['threshold_mb']}MB)، القرار الصحيح هو استخدام Python Batch وليس هذا المحرك."
        )

    load_spark(args.input, ctx)