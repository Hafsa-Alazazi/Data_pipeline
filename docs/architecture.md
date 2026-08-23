# Architecture

## Overview

The pipeline ingests a single CSV file of e-commerce order data and produces
three MongoDB collections: `orders_raw` (untouched copy), `orders_validated`
(usable records), and `orders_quarantine` (records that could not be safely
corrected). It follows an ELT pattern: raw data is loaded first, quality
rules are applied afterwards, never before.

## Data flow

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

## Why two loading engines

Below the configured threshold (default 200MB), a single-threaded Python
process reading the file with `csv.DictReader` (streaming, never loading the
full file into memory) and writing to MongoDB in configurable batches via
`insert_many` is simpler to operate and faster in practice, since it avoids
JVM startup overhead (roughly 10-15 seconds) that Spark always pays
regardless of file size.

Above the threshold, PySpark reads the file with a fixed schema (all
columns as `StringType`, deliberately avoiding `inferSchema` so dirty raw
values are preserved unmodified for the Raw layer) and writes to MongoDB in
parallel via the MongoDB Spark Connector, splitting the input into multiple
partitions automatically based on file size.

## Why the ELT pattern (raw before clean)

Loading everything into `orders_raw` unmodified first, before any quality
rule runs, guarantees a traceable, replayable copy of the data exactly as it
arrived - independent of any bug or change later in the quality rules
themselves. If a quality rule is later found to be wrong, the raw layer can
be re-processed without re-ingesting the source file.

## Why per-field pure functions for quality rules

Each cleaning rule in `src/quality_rules.py` (Arabic digit conversion,
currency normalization, phone normalization, email repair, date
normalization, status synonym mapping, JSON validation) is a pure function:
same input always produces the same output, no side effects, no database or
I/O calls. This keeps them independently unit-testable (see
`tests/test_cleaning_rules.py`) and keeps the loading logic, cleaning logic,
and database logic fully separated, as required by the course spec.

## Why Upsert with a content hash, not insert-then-check

`orders_validated` uses `id_order` as the stable business key, backed by a
unique index. Instead of checking "does this key already exist?" before
deciding whether to insert or update (a race-prone, two-step pattern), each
cleaned record's content (excluding `id_order` itself) is hashed. A single
`bulk_write` with `ReplaceOne(..., upsert=True)` per record either inserts a
new document, replaces an existing one only if the hash actually changed, or
does nothing if the hash is identical to what's already stored. This is the
mechanism that guarantees idempotency: re-running the exact same file twice
produces zero new inserts and zero unnecessary writes on the second run.

## Why quarantine instead of dropping bad records

Every raw record must end up in exactly one of three states: valid,
corrected, or quarantined - never silently dropped. Quarantined records
keep their original raw content plus one or more error codes explaining why
they couldn't be corrected safely (e.g. a negative quantity inside
`items_json`, which could mean a return, a data entry error, or something
else entirely - the pipeline does not guess). This makes every run
auditable: `run_raw_count = count_valid + count_corrected + count_quarantine`
is checked and enforced on every single run in `src/elt_pipeline.py`.

## Known limitation: the ELT stage is sequential

The Quality & Transform + Classification stage runs as a single-threaded
Python loop regardless of which engine loaded the data. This means PySpark
only accelerates the *loading* stage, not cleaning/classification. Measured
on real data, this stage took roughly 41 minutes for 2 million rows,
suggesting the full ~12GB file (~29 million rows) could take 8-10 hours for
classification alone. A future iteration could distribute this stage itself
using Spark UDFs or Python multiprocessing.