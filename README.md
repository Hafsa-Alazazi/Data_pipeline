# Hybrid Data Pipeline for Order Data Processing

Builds a hybrid data pipeline that ingests a dirty e-commerce order CSV file,
automatically chooses between **Python Batch Loading** (small files) and
**Apache PySpark** (large files) based on file size, then applies an **ELT**
pattern (load raw first, clean and classify afterwards), guaranteeing
**Idempotency** and **Upsert** semantics on re-runs, and quarantining any
record that cannot be safely corrected instead of dropping it.

---

## 1. Architecture

```
Provided Dirty CSV
        |
        v
File Router: size <= threshold ?
   |                    |
   v                    v
Python Batch          PySpark
   |                    |
   +---------+----------+
             v
        orders_raw
             |
             v
     Cleaning + Validation
        |            |
        |            +--> orders_quarantine
        v
   Idempotent Upsert
        |
        v
   orders_validated

Metrics -> reports/results.json
```

The eight pipeline stages (per the official assignment spec):

| # | Stage | Responsible file |
|---|---|---|
| 1 | File discovery + generate `id_run` | `src/file_router.py` |
| 2 | Engine selection (Router) | `src/file_router.py` |
| 3 | Load Raw (no quality filtering) | `src/batch_loader.py` or `src/spark_loader.py` |
| 4 | Quality & Transform | `src/quality_rules.py` |
| 5 | Classification (Valid/Corrected/Quarantine) | `src/quality_rules.py` |
| 6 | Final Load (Upsert) | `src/elt_pipeline.py` |
| 7 | Idempotency Check | Proven by re-running `main.py` on the same file |
| 8 | Metrics | `src/metrics.py` → `reports/results.json` |

---

## 2. Prerequisites

| Software | Version actually tested | Note |
|---|---|---|
| Python | 3.12 | |
| Java (JDK) | 17 (Temurin) | Required to run PySpark |
| Apache Spark (via PySpark) | 4.2.0 | Installed automatically via `pip` |
| MongoDB Community Server | Any recent version | Must be running before execution |
| MongoDB Compass (optional) | - | For visually inspecting the data |

### Install Python dependencies

```bash
pip install -r requirements.txt
```

### ⚠️ Windows-only extra setup: winutils.exe

On Windows, Spark requires a dummy `winutils.exe` even for plain local
filesystem operations (not just HDFS), otherwise it fails with
`HADOOP_HOME and hadoop.home.dir are unset`.

1. Download these two files from the trusted `cdarlint/winutils` GitHub repo:
   - `https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe`
   - `https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll`
2. Place both inside: `C:\hadoop\bin\`
3. **No manual environment variable setup is needed** — `src/spark_loader.py`
   automatically sets `HADOOP_HOME` on Windows if that folder exists, and prints:
   ```
   [spark_loader] HADOOP_HOME automatically set to: C:\hadoop
   ```

No extra step is needed on Linux/Mac.

---

## 3. Running the Project

### a) Create a small, reproducible sample (required first)

```bash
python src/create_small_sample.py --input data/orders_huge_mixed_quality.csv --rows 100000
```

Produces `data/orders_sample_100000.csv`. Row count is configurable via
`--rows`, and the output path via `--output` (optional).

### b) Full run (single unified entry point)

```bash
python main.py --input data/orders_sample_100000.csv
```

`main.py` is the **only** entry point for the whole project: it calls the
Router, automatically picks the Batch or Spark loader based on file size,
then runs ELT, then records metrics. No other file is meant to be run
separately to perform the full task.

### c) Forcing a specific engine for testing (optional)

The default threshold is 200MB (`SMALL_FILE_THRESHOLD_MB` in
`config/settings.py`). To test the PySpark path on a smaller file without
waiting for an actual huge file:

```bash
# PowerShell
$env:SMALL_FILE_THRESHOLD_MB = "10"
python main.py --input data/orders_sample_100000.csv

# CMD
set SMALL_FILE_THRESHOLD_MB=10
python main.py --input data\orders_sample_100000.csv
```

**Note:** in PowerShell, the `set` command does **not** set a real
environment variable (only CMD does); use `$env:VAR = "value"` in PowerShell.

### d) Why a 200MB threshold?

200MB was chosen as a practical balance: smaller than the typical default
block size in most distributed filesystems (128-256MB), yet large enough to
avoid paying Spark's fixed JVM startup cost (roughly 10-15 seconds) on small
files where Python Batch is actually faster due to the absence of that
overhead.

---

## 4. Idempotency & Upsert Proof (mandatory deliverable)

Proven in practice with three consecutive runs on `orders_sample_100000.csv`
after clearing all three MongoDB collections:

| Run | Description | `count_inserted` | `count_updated` | `count_unchanged` |
|---|---|---|---|---|
| 1 | First run on an empty database | 90,902 | 0 | 0 |
| 2 | Re-running the exact same file, unmodified | 0 | 0 | 90,902 |
| 3 | Modifying `payment_status` on one existing record, then re-running | 0 | **1** | 90,901 |

**To reproduce this test:**

```bash
# 1) Clear the three collections from mongosh:
#    use midterm_pipeline
#    db.orders_raw.deleteMany({})
#    db.orders_validated.deleteMany({})
#    db.orders_quarantine.deleteMany({})

# 2) First and second run
python main.py --input data/orders_sample_100000.csv
python main.py --input data/orders_sample_100000.csv

# 3) Real Update test (targets a record that genuinely exists in orders_validated)
python tests/create_update_test_file.py --input data/orders_sample_100000.csv --output data/orders_update_test.csv
python main.py --input data/orders_update_test.csv
```

Upsert mechanism: the stable business key is `id_order`, backed by a Unique
Index on it inside `orders_validated` (created automatically in
`src/mongo_setup.py`). A `content_hash` is computed per record (excluding
`id_order` itself); if the key doesn't exist yet, a new record is inserted
(`inserted`); if it exists but the hash differs, it's updated (`updated`);
if it exists and the hash matches, nothing is written (`unchanged`) - this
is the basis of guaranteeing Idempotency without relying on
`insert-then-check`.

---

## 5. Running the Test Suite

Unit tests cover every individual cleaning rule (`tests/test_cleaning_rules.py`)
and the full record classification logic (`tests/test_classification.py`),
as required by the assignment spec (Section 9: "tests must be added for the
core cleaning and classification rules").

```bash
pytest tests/test_cleaning_rules.py tests/test_classification.py -v
```

Expected result: **43 passed**. These tests are pure unit tests - they do
not require MongoDB, Spark, or any network access to run.

---

## 6. Project Structure

```
midterm-data-pipeline/
|-- main.py                          # The single unified entry point
|-- README.md
|-- requirements.txt
|-- config/
|   `-- settings.py                  # All configurable settings (no hardcoded values in code)
|-- data/
|   |-- orders_huge_mixed_quality.csv   # Original provided file (not committed, too large)
|   `-- orders_sample_*.csv             # Samples generated via create_small_sample.py
|-- src/
|   |-- file_router.py               # Automatic engine selection based on size
|   |-- create_small_sample.py       # Extracts a small sample (streaming, no Excel)
|   |-- batch_loader.py              # Python Batch engine (streaming + batches)
|   |-- spark_loader.py              # PySpark engine (fixed schema + parallel write)
|   |-- quality_rules.py             # 8+ cleaning rules + audit trail + classification
|   |-- elt_pipeline.py              # Consistency check + Upsert + Idempotency
|   |-- mongo_setup.py               # Creates collections and indexes (unique on id_order)
|   `-- metrics.py                   # Aggregates and saves metrics to results.json
|-- tests/
|   |-- test_cleaning_rules.py       # 25 unit tests for individual cleaning rules
|   |-- test_classification.py       # 18 tests for full-record classification logic
|   `-- create_update_test_file.py   # Standalone script proving the Update path
|-- reports/
|   |-- results.json                 # Log of every run (auto-generated, cumulative list)
|   |-- results.md                   # Batch vs PySpark comparison report
|   `-- screenshots/                 # Spark UI and MongoDB Compass screenshots
`-- docs/
    `-- architecture.md
```

---

## 7. MongoDB Collections

| Collection | Content | Indexes |
|---|---|---|
| `orders_raw` | Every record exactly as received, no quality filtering | Regular index on `id_run` (no unique constraint) |
| `orders_validated` | Valid / corrected, usable records | **Unique index on `id_order`** |
| `orders_quarantine` | Records that couldn't be safely corrected, with error codes and reasons | Regular index on `id_run` and `id_order` |

Default database name: `midterm_pipeline` (configurable via `MONGO_URI` and
`MONGO_DB_NAME` in `config/settings.py` or environment variables).

---

## 8. Important Performance Note (documented from real measurements)

Actual measurements on real data from the original file:

| Sample size | Load time | Cleaning/Classification (ELT) time |
|---|---|---|
| 100,000 rows (~42MB) | ~5s (Batch) / ~19s (Spark) | ~60-105s |
| 2,000,000 rows (~839MB) | ~99s (Spark, 7 partitions) | ~2487s (~41 min) |

**Architectural note:** the Quality & Transform + Classification stage
(`quality_rules.py` and `elt_pipeline.py`) runs as sequential Python logic
regardless of which loading engine was used (Batch or Spark) - meaning
PySpark only speeds up **loading**, not cleaning. Extrapolating linearly
from the measurement above, processing the full file (~12GB, roughly 29
million rows) could take 8-10 hours for the ELT stage alone. This is a real
architectural bottleneck worth addressing in a future iteration (e.g. by
distributing the classification logic itself onto Spark using UDFs, or via
Python multiprocessing instead of the current sequential loop).

---

## 9. Explicitly Handled Error Cases

- A full batch failure in the Batch Loader: the reason is logged and never
  silently swallowed, and execution stops clearly (`try/except` without a
  silent `pass`).
- MongoDB connection failure: a clear message is printed via `main.py` and
  the program exits with a non-zero status instead of continuing silently.
- Consistency equation failure (Section 6.11 of the official spec):
  `run_raw_count = count_valid + count_corrected + count_quarantine`
  immediately halts execution (`AssertionError`) instead of continuing with
  inconsistent data.