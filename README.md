# Hybrid Data Pipeline for Order Data Processing - Final Project (Phase 2)

Big Data course (Practical) | Al-Razi University
AI Track, Level 4 | Instructor: Eng. Omar Abu Sind

**Part 1 - Midterm:** the ELT data pipeline (Sections 1-10).
**Part 2 - Final project:** MongoDB queries and indexes with Explain, aggregation
reports, incremental materialized views, scheduled jobs and a unified FastAPI
interface on top of the same pipeline (Section 11).

The midterm pipeline builds a hybrid data pipeline that ingests a dirty e-commerce order CSV file,
automatically chooses between **Python Batch Loading** (small files) and
**Apache PySpark** (large files) based on file size, then applies an **ELT**
pattern (load raw first, clean and classify afterwards), guaranteeing
**Idempotency** and **Upsert** semantics on re-runs, and quarantining any
record that cannot be safely corrected instead of dropping it.

---

## Quick Start (copy and paste)

Requirements: Python 3.12, Java 17 (needed by PySpark, see Section 2) and a
running MongoDB (default `mongodb://localhost:27017`). On Windows use `py`
instead of `python` if `python` is not recognized.

```bash
# 1) Install dependencies
pip install -r requirements.txt

# 2) Optional: copy the config template (every value has a default; no secrets inside)
cp .env.example .env                    # Windows: copy .env.example .env

# 3) Start the API (also starts the scheduled jobs)
python -m uvicorn api:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000/docs** (Swagger UI) and run the endpoints in this
order - or use the `curl` commands below from a second terminal:

```bash
# 1) Load a CSV file through the midterm pipeline (use ANY order CSV file)
curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" \
     -d '{"input_path": "path/to/your_file.csv"}'
# 2) Create the indexes
curl -X POST http://localhost:8000/indexes
# 3) Build / refresh the materialized views (run twice: the 2nd run finds nothing new)
curl -X POST http://localhost:8000/refresh-mv
# 4) Explore
curl http://localhost:8000/health
curl http://localhost:8000/queries
curl http://localhost:8000/aggregations
curl http://localhost:8000/aggregations/sales_by_city
curl http://localhost:8000/jobs
curl -X POST http://localhost:8000/jobs/refresh_materialized_views/run
```

> PowerShell: `curl` is an alias of `Invoke-WebRequest`; use `curl.exe`, or
> simply use the Swagger UI at `/docs`, which needs no commands.

Without the API, the midterm pipeline still runs directly from the command
line on any CSV file (small or large, engine chosen automatically):

```bash
python main.py --input path/to/your_file.csv
```

Details of every Phase 2 part (queries, indexes + Explain, aggregations,
materialized views, jobs, endpoints) are in
[Section 11](#11-final-project-phase-2).

Testing with a different data file than the one used to build this
project? See Section 5 for column requirements and troubleshooting notes
(including a Windows `python` vs `py` command note).

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

## 5. Running on a New / Unfamiliar Data File (e.g. instructor-provided)

The original `data/orders_huge_mixed_quality.csv` is intentionally **not
committed to this repository** (it is several gigabytes; see `.gitignore`).
If you are testing this project with a different CSV file:

```bash
python main.py --input path/to/any_file.csv
```

`main.py` works on **any** CSV file, large or small — the engine (Python
Batch vs PySpark) is chosen automatically based on that file's size alone.
There is no requirement to use the exact original file name, and no other
file needs to exist beforehand. The only requirement is that the file
follows the same column structure this project was built for (`order_id`,
`order_date`, `status`, `customer_id`, `customer_name`, `customer_phone`,
`customer_email`, `city`, `district`, `delivery_type`, `delivery_cost`,
`payment_method`, `payment_status`, `payment_amount`, `currency`,
`total_amount`, `items_json`) — matching the header row `src/quality_rules.py`
and the fixed schema in `src/spark_loader.py` expect. Data *values* can be
arbitrarily dirty; column *names* need to match.

If the provided file is large enough to trigger the PySpark path
(Section 3d) and its columns are present but in a **different order** than
listed above, `src/spark_loader.py` is configured to fail loudly with a
clear schema-mismatch error rather than silently misaligning column values
- rerun on the small-file threshold path (Python Batch, which matches
columns by name regardless of order) or reorder the columns to match if
this happens.

**Troubleshooting `python` not found (Windows):** some Windows setups only
register the `py` launcher, not a `python` alias, especially in a freshly
opened terminal window. If any command below gives
`'python' is not recognized as the name of a cmdlet...`, use `py` instead
of `python` for every command in this document (e.g. `py main.py --input ...`).

---

## 6. Running the Test Suite

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

## 7. Project Structure

```
Data_pipeline/
|-- main.py                          # The single unified entry point
|-- api.py                           # FINAL PROJECT: unified FastAPI interface
|-- .env.example                     # Environment variable template (no secrets)
|-- README.md
|-- requirements.txt
|-- config/
|   `-- settings.py                  # All configurable settings (no hardcoded values in code)
|-- data/
|   |-- .gitkeep                        # Placeholder only - see below
|   |-- orders_huge_mixed_quality.csv   # NOT committed (too large, ~12GB) - place your own file anywhere and point --input at it
|   `-- orders_sample_*.csv             # Also not committed - regenerate via create_small_sample.py if needed
|-- src/
|   |-- file_router.py               # Automatic engine selection based on size
|   |-- create_small_sample.py       # Extracts a small sample (streaming, no Excel)
|   |-- batch_loader.py              # Python Batch engine (streaming + batches)
|   |-- spark_loader.py              # PySpark engine (fixed schema + parallel write)
|   |-- quality_rules.py             # 8+ cleaning rules + audit trail + classification
|   |-- elt_pipeline.py              # Consistency check + Upsert + Idempotency
|   |-- mongo_setup.py               # Creates collections and indexes (unique on id_order)
|   |-- metrics.py                   # Aggregates and saves metrics to results.json
|   `-- phase2/                      # FINAL PROJECT additions (see Section 11)
|       |-- queries.py               # 5 practical queries
|       |-- indexes.py               # 5 indexes (1 compound)
|       |-- explain_report.py        # executionStats before/after -> docs/explain_report.md
|       |-- aggregations.py          # 5 aggregation reports
|       |-- materialized_views.py    # 2 incremental materialized views
|       `-- jobs.py                  # 2 scheduled jobs + run log
|-- tests/
|   |-- test_cleaning_rules.py       # 25 unit tests for individual cleaning rules
|   |-- test_classification.py       # 18 tests for full-record classification logic
|   `-- create_update_test_file.py   # Standalone script proving the Update path
|-- reports/
|   |-- results.json                 # Log of every run (auto-generated, cumulative list)
|   |-- results.md                   # Batch vs PySpark comparison report
|   `-- screenshots/                 # Spark UI and MongoDB Compass screenshots
`-- docs/
    |-- architecture.md
    `-- explain_report.md            # Generated: explain before/after indexes
```

---

## 8. MongoDB Collections

| Collection | Content | Indexes |
|---|---|---|
| `orders_raw` | Every record exactly as received, no quality filtering | Regular index on `id_run` (no unique constraint) |
| `orders_validated` | Valid / corrected, usable records | **Unique index on `id_order`** |
| `orders_quarantine` | Records that couldn't be safely corrected, with error codes and reasons | Regular index on `id_run` and `id_order` |

Default database name: `midterm_pipeline` (configurable via `MONGO_URI` and
`MONGO_DB_NAME` in `config/settings.py` or environment variables).

---

## 9. Important Performance Note (documented from real measurements)

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

## 10. Explicitly Handled Error Cases

- A full batch failure in the Batch Loader: the reason is logged and never
  silently swallowed, and execution stops clearly (`try/except` without a
  silent `pass`).
- MongoDB connection failure: a clear message is printed via `main.py` and
  the program exits with a non-zero status instead of continuing silently.
- Consistency equation failure (Section 6.11 of the official spec):
  `run_raw_count = count_valid + count_corrected + count_quarantine`
  immediately halts execution (`AssertionError`) instead of continuing with
  inconsistent data.

---

## 11. Final Project (Phase 2)

Phase 2 adds new functionality on top of the midterm pipeline **without
changing it**. Everything runs against the `orders_validated` collection that
the midterm pipeline produces, so **ingest data first** (Section 3, or
`POST /ingest`). Nothing in Phase 2 depends on file names, record counts or
fixed values: all numbers are computed from whatever data is in the database.

### 11.1 Setup and running the API

```bash
pip install -r requirements.txt
cp .env.example .env         # optional (Windows CMD / PowerShell: copy .env.example .env)
# make sure MongoDB is running, then:
python -m uvicorn api:app --host 0.0.0.0 --port 8000
```

> **Windows note:** if `pip` / `python` is not recognized in PowerShell, use the
> `py` launcher instead: `py -m pip install -r requirements.txt` and
> `py -m uvicorn api:app --port 8000`. Always prefer `python -m uvicorn` over
> plain `uvicorn` (the latter is often missing from PATH).

* Swagger UI: `http://localhost:8000/docs`
* All responses are JSON.
* The scheduler starts automatically with the API (set `ENABLE_SCHEDULER=false`
  in `.env` to disable it).

Recommended order for a fresh database:

```text
1. POST /ingest   {"input_path": "path/to/file.csv"}   -> loads orders_validated
2. POST /indexes                                         -> creates the 5 indexes
3. POST /refresh-mv                                      -> builds the materialized views
4. GET  /queries, /aggregations, /jobs                   -> explore the results
```

### 11.2 API endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | MongoDB connectivity, collection counts, scheduler state |
| POST | `/ingest` | Body `{"input_path": "..."}` - runs the **same** midterm pipeline (`main.run_pipeline`) |
| POST | `/ingest/upload` | Same pipeline, but the CSV is uploaded as a file (saved to `data/uploads/`) |
| POST | `/indexes` | Creates the Phase 2 indexes (safe to repeat). `?with_explain=true` also regenerates `docs/explain_report.md` |
| GET | `/queries` | Lists the 5 queries and their parameters |
| GET | `/queries/{name}` | Runs a query; parameters as query string. `?explain=true` returns executionStats instead |
| GET | `/aggregations` | Lists the 5 aggregation reports |
| GET | `/aggregations/{name}` | Runs a report; optional parameters as query string |
| POST | `/refresh-mv` | Incremental refresh of both materialized views. `?full=true` forces a full rebuild |
| GET | `/jobs` | Scheduled jobs, their schedules, last run and recent run history |
| POST | `/jobs/{name}/run` | Runs a job immediately (manual trigger, logged like a scheduled run) |

Errors use proper HTTP codes: `404` unknown name/file, `400` missing or
invalid parameter, `503` MongoDB unavailable.

### 11.3 Queries (`src/phase2/queries.py`)

| Name | Parameters | Example |
|---|---|---|
| `find_orders_by_city` | `city`, `limit` | `GET /queries/find_orders_by_city?city=<city>` |
| `find_orders_by_date_range` | `start_date`, `end_date` (YYYY-MM-DD), `limit` | `GET /queries/find_orders_by_date_range?start_date=2025-01-01&end_date=2025-01-31` |
| `find_orders_by_status_and_payment` | `status`, `payment_status`, `limit` | `GET /queries/find_orders_by_status_and_payment?status=<s>&payment_status=<p>` |
| `find_top_orders_by_amount` | `limit` | `GET /queries/find_top_orders_by_amount?limit=10` |
| `find_orders_by_customer` | `customer_id`, `limit` | `GET /queries/find_orders_by_customer?customer_id=<id>` |

Dates are stored as ISO strings (`YYYY-MM-DDTHH:MM:SS`) in `orders_validated`,
so range queries compare strings, which preserves chronological order.

### 11.4 Indexes and Explain (`src/phase2/indexes.py`, `explain_report.py`)

| Index | Keys | Serves | Why |
|---|---|---|---|
| `idx_city` | `city` | `find_orders_by_city` | Equality lookup |
| `idx_order_date` | `order_date` | `find_orders_by_date_range` | Range scan instead of full collection scan |
| `idx_status_payment_status` | `status, payment_status` (**Compound**) | `find_orders_by_status_and_payment` | Query filters on both fields together |
| `idx_total_amount` | `total_amount` desc | `find_top_orders_by_amount` | Sorted index removes the in-memory sort |
| `idx_customer_id` | `customer_id` | `find_orders_by_customer` | Frequent equality lookup |

`explain("executionStats")` is run for **3 queries before and after** the
indexes (status+payment, top by amount, by customer). Generate the report with:

```bash
python -m src.phase2.explain_report      # writes docs/explain_report.md
# or: POST /indexes?with_explain=true
```

The script temporarily drops the Phase 2 indexes to measure the "before" state,
then recreates them. It takes values (status, customer...) from the live data.
Each query is executed once as a warm-up and measured on the second run, so
cold-cache effects do not distort the timings; the number of documents
examined is the most reliable metric. A sample report from the development run is in `docs/explain_report.md`.

### 11.5 Aggregation reports (`src/phase2/aggregations.py`)

| Name | Description | Optional params |
|---|---|---|
| `sales_by_city` | Total sales, order count, average order value per city | `limit` |
| `top_products` | Best products by revenue and quantity | `limit` |
| `top_customers` | Highest-spending customers | `limit` |
| `sales_by_period` | Sales per month or day | `granularity=month\|day` |
| `orders_by_status` | Order distribution by status | - |

Run one with `GET /aggregations/{name}` or all of them from the command line:
`python -m src.phase2.aggregations`.

### 11.6 Materialized views (`src/phase2/materialized_views.py`)

| View (collection) | Built from | Content |
|---|---|---|
| `daily_sales_summary` | `sales_by_period` (day) | Sales, order count, average order value per day |
| `top_products_summary` | `top_products` | Quantity, revenue and order count per product |

**Incremental refresh.** The midterm pipeline already sets `updated_at` on a
record only when its content really changed (content-hash upsert). Each view
keeps a *watermark* (`phase2_mv_meta`). A refresh finds only the records whose
`updated_at` is newer than the watermark, determines which days / SKUs they
affect, recomputes **only those** using the same aggregation pipelines, and
upserts them. Untouched days and products are not recalculated. The first run
(or an empty view, or `?full=true`) does a full build.

```bash
python -m src.phase2.materialized_views            # incremental
python -m src.phase2.materialized_views --full     # full rebuild
```

Known limitation: if an existing order changes its date or its products, the
old day/product keeps its previous value until a full refresh
(`--full` or `POST /refresh-mv?full=true`).

### 11.7 Scheduled jobs (`src/phase2/jobs.py`)

| Job | Schedule (configurable in `.env`) | What it does |
|---|---|---|
| `refresh_materialized_views` | every `JOB_REFRESH_MV_EVERY_MINUTES` (15) | Incremental refresh of both views |
| `periodic_report` | daily at `JOB_REPORT_HOUR:JOB_REPORT_MINUTE` (02:00) | Runs the 5 aggregations, stores the snapshot in MongoDB and exports `reports/periodic_reports/*.json` |

Every run (scheduled or manual) is logged in the `phase2_job_runs` collection
with job name, trigger, **start time, end time**, duration, **status
(success/failed)** and the result or error.

```bash
# Manual runs for testing
python -m src.phase2.jobs list
python -m src.phase2.jobs run refresh_materialized_views
python -m src.phase2.jobs run periodic_report
python -m src.phase2.jobs history
python -m src.phase2.jobs serve          # scheduler only, without the API
# Or via the API: POST /jobs/{name}/run  and  GET /jobs
```

### 11.8 New MongoDB collections

`daily_sales_summary`, `top_products_summary`, `phase2_mv_meta` (watermarks),
`phase2_job_runs` (job log), `phase2_periodic_reports` (periodic snapshots).