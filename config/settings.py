"""
config/settings.py
--------------------
كل الإعدادات المهمة للمشروع في مكان واحد (بدل توزيعها داخل الكود).
يمكن التحكم بأي قيمة هنا عبر متغيرات البيئة (Environment Variables) أيضًا،
وإن لم توجد، تُستخدم القيمة الافتراضية.

مبرر اختيار SMALL_FILE_THRESHOLD_MB = 200:
    - الهدف من الحد الفاصل هو الفصل بين ما يمكن معالجته بأمان بذاكرة عادية
      (~4-8GB RAM) باستخدام قراءة Streaming بـ Python، وما يحتاج فعليًا
      معالجة موزعة (Distributed) بـ Spark.
    - ملف بحجم 200MB نصي (CSV) يحتوي عادة ملايين قليلة من الصفوف، وقراءته
      سطرًا بسطر (Streaming) لا تستهلك أكثر من عشرات الميجابايت من الذاكرة
      الفعلية أثناء التنفيذ، لذلك يبقى ضمن قدرة Python Batch دون مشاكل.
    - أي ملف أكبر من ذلك (وملفنا 12GB بالمشروع) يصبح غير عملي لمعالجته
      تسلسليًا خلال وقت معقول، فنحتاج للتوازي الذي يوفره PySpark.
"""

import os

# --------------------------------------------------------------------------
# 1) إعدادات اختيار المحرك (Router)
# --------------------------------------------------------------------------
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", 200))

# --------------------------------------------------------------------------
# 2) إعدادات Python Batch Loader
# --------------------------------------------------------------------------
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))          # حجم الدفعة الواحدة لكل insert_many
CSV_ENCODING = os.getenv("CSV_ENCODING", "utf-8-sig")  # utf-8-sig يزيل BOM تلقائيًا

# --------------------------------------------------------------------------
# 3) إعدادات PySpark Loader
# --------------------------------------------------------------------------
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "MidtermDataPipeline")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")     # local[*] للتشغيل الفردي (غير Cluster)
SPARK_MONGO_WRITE_BATCH_SIZE = int(os.getenv("SPARK_MONGO_WRITE_BATCH_SIZE", 10000))

# مسار احتياطي لـ HADOOP_HOME على أنظمة Windows فقط (يُستخدم تلقائيًا في
# src/spark_loader.py إن لم يكن HADOOP_HOME مضبوطًا مسبقًا كمتغير بيئة
# نظام). راجع README.md لتعليمات تحميل winutils.exe ووضعه هنا.
HADOOP_HOME_WINDOWS_FALLBACK = os.getenv("HADOOP_HOME_WINDOWS_FALLBACK", r"C:\hadoop")

# --------------------------------------------------------------------------
# 4) إعدادات الاتصال بـ MongoDB
# --------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "midterm_pipeline")

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"

# --------------------------------------------------------------------------
# 5) مسارات المشروع
# --------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
RESULTS_JSON_PATH = os.path.join(REPORTS_DIR, "results.json")

# --------------------------------------------------------------------------
# 6) إعدادات عامة
# --------------------------------------------------------------------------
DEFAULT_CURRENCY = "YER"
LOG_EVERY_N_BATCHES = int(os.getenv("LOG_EVERY_N_BATCHES", 1))  # كل كم دفعة نطبع تقدم