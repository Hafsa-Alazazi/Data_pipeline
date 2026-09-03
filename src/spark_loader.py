

import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType



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
    
    _ensure_hadoop_home_on_windows()

    spark = (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config("spark.mongodb.write.connection.uri", settings.MONGO_URI)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  
    return spark


def load_spark(file_path: str, run_context: dict) -> dict:
    
    spark = build_spark_session()
    start_time = time.time()

    df = (
        spark.read
        .option("header", True)
        .option("escape", '"')

        .option("enforceSchema", "false")
        # إصلاح أمان مهم: enforceSchema=false يجبر Spark على مقارنة أسماء
        # أعمدة الـ header الفعلية بالملف مع أسماء ORDERS_CSV_SCHEMA بنفس
        # الترتيب، ويفشل بخطأ واضح لو غير متطابقة. الإعداد الافتراضي
        # (enforceSchema=true) كان يتجاهل الـ header تمامًا ويربط القيم
        # بالموقع (الترتيب) فقط - لو جاء ملف من مصدر آخر بنفس أسماء
        # الأعمدة لكن بترتيب مختلف، كانت البيانات ستُقرأ بصمت وبشكل خاطئ
        # تمامًا (مثلاً قيمة "currency" تُقرأ في عمود "payment_status")
        # دون أي تحذير - وهذا أخطر بكثير من فشل صريح فورًا.
        .schema(ORDERS_CSV_SCHEMA)   # Schema ثابتة، بدون inferSchema
>>>>>>> d0597aa (Add Quick Start section and instructions for testing with a new data file)
        .csv(file_path)
        
    )

    
    partitions_input = df.rdd.getNumPartitions()


    try:
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
>>>>>>> d0597aa (Add Quick Start section and instructions for testing with a new data file)
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

        return summary
    except Exception:
        # لا نُخفي سبب الفشل أبدًا (القسم 9: معالجة الأخطاء دون إخفاء
        # السبب) - نطبع رسالة واضحة، ثم نعيد رفع الاستثناء نفسه ليتوقف
        # main.py بوضوح بدل الاستمرار ببيانات جزئية.
        print("[spark_loader] ❌ فشل أثناء التحميل أو الكتابة بواسطة PySpark.")
        raise
    finally:
        # يُغلق SparkSession دائمًا - سواء نجحت العملية أو فشلت - لمنع
        # بقاء عمليات JVM معلّقة في الخلفية (القسم 9: إغلاق Spark وMongoDB
        # بصورة سليمة عبر try/finally).
        spark.stop()
        print("[spark_loader] تم إغلاق SparkSession بأمان.")


if __name__ == "__main__":
    
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