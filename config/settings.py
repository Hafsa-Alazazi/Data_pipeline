import os

# تحميل ملف .env (إن وُجد) من جذر المشروع قبل قراءة أي متغير بيئة.
# python-dotenv اختياري هنا: غيابه لا يكسر المشروع، فقط لا يُقرأ .env.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", 200))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))          
CSV_ENCODING = os.getenv("CSV_ENCODING", "utf-8-sig")  

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "MidtermDataPipeline")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")     
SPARK_MONGO_WRITE_BATCH_SIZE = int(os.getenv("SPARK_MONGO_WRITE_BATCH_SIZE", 10000))

HADOOP_HOME_WINDOWS_FALLBACK = os.getenv("HADOOP_HOME_WINDOWS_FALLBACK", r"C:\hadoop")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "midterm_pipeline")

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
RESULTS_JSON_PATH = os.path.join(REPORTS_DIR, "results.json")


DEFAULT_CURRENCY = "YER"
LOG_EVERY_N_BATCHES = int(os.getenv("LOG_EVERY_N_BATCHES", 1))