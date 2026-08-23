# Results Report: Python Batch vs PySpark

All numbers below come from actual runs against real data extracted from the
provided `orders_huge_mixed_quality.csv` file, recorded automatically in
`reports/results.json`. No numbers here are estimated or simulated unless
explicitly marked as "extrapolated."

## 1. Loading engine comparison

| Metric | Python Batch (100,000 rows / 41.77MB) | PySpark (100,000 rows / 41.77MB, threshold forced to 10MB) | PySpark (2,000,000 rows / 839.03MB) |
|---|---|---|---|
| Engine chosen | `python_batch` | `pyspark` | `pyspark` |
| Load time | ~4.6-5.4s | ~19.3s | ~98.6s |
| Throughput (rows/sec) | ~20,000-22,000 | ~5,184 | ~20,292 |
| Input partitions | N/A (sequential batches of 5,000) | 4 | 7 |

**Observation:** on the same 100,000-row file, Python Batch is actually
faster than PySpark (5s vs 19s). This is expected: Spark pays a fixed JVM
startup and session initialization cost (roughly 10-15 seconds) regardless
of file size, which dominates the total time on small inputs. PySpark's
advantage only appears once the file is large enough (2,000,000 rows here)
that this fixed cost becomes negligible relative to genuine parallel I/O -
throughput at that scale matches the Batch engine's per-row rate despite
processing 20x more data, because the work is now split across multiple
partitions instead of running as one sequential stream.

This is exactly why the file-size threshold (`SMALL_FILE_THRESHOLD_MB=200`)
exists and is justified in `README.md`: below it, Python Batch wins; above
it, PySpark wins.

## 2. Full pipeline runs and Idempotency/Upsert proof

Three consecutive runs on `orders_sample_100000.csv`, after clearing all
three MongoDB collections:

| Run | Description | count_inserted | count_updated | count_unchanged | Consistency check |
|---|---|---|---|---|---|
| 1 | First run, empty database | 90,902 | 0 | 0 | ✅ 100,000 = 159 + 90,743 + 9,098 |
| 2 | Re-run, identical file | 0 | 0 | 90,902 | ✅ same equation |
| 3 | One record's `payment_status` modified, then re-run | 0 | 1 | 90,901 | ✅ same equation |

This demonstrates:
- **Idempotency**: re-running unmodified data produces zero new inserts and
  zero unnecessary updates.
- **Upsert correctness**: a genuine change to one existing record is
  detected and updated in isolation, without affecting or duplicating any
  other record.

## 3. Classification breakdown (2,000,000-row run)

```
run_raw_count      = 2,000,000
count_valid        = 3,191
count_corrected    = 1,814,375
count_quarantine   = 182,434

Consistency equation: 2,000,000 = 3,191 + 1,814,375 + 182,434  ✅
```

Quarantine reason breakdown (`counts_case_error`):

| Error code | Count |
|---|---|
| DATE_IMPOSSIBLE_INVALID | 58,360 |
| ID_CUSTOMER_MISSING | 28,092 |
| JSON_ITEMS_CORRUPTED | 27,764 |
| ID_ORDER_DUPLICATE | 27,548 |
| EMAIL_UNFIXABLE | 14,035 |
| VALUE_NEGATIVE_AMBIGUOUS | 13,802 |
| ITEMS_EMPTY | 13,802 |
| ID_ORDER_MISSING | 13,961 |

(Note: a single record can trigger more than one error code when
`ERRORS_CONFLICTING_MULTIPLE` applies, so these counts are not expected to
sum exactly to `count_quarantine`.)

## 4. ELT (cleaning + classification) performance and a real bottleneck

| Sample size | ELT time | Rows/sec |
|---|---|---|
| 100,000 rows | ~60-105s | ~950-1,660 |
| 2,000,000 rows | ~2,487s (~41.4 min) | ~804 |

**Important finding:** unlike the loading stage, ELT (`src/quality_rules.py`
+ `src/elt_pipeline.py`) runs as sequential single-threaded Python
regardless of which engine loaded the data. Loading with PySpark does
**not** speed up cleaning or classification at all.

**Extrapolated estimate (not measured, calculated only):** the full
~12GB source file is roughly 29 million rows (linear scaling from the
measured ~840MB / 2,000,000-row sample). At the measured ELT rate of
~804-950 rows/sec, processing the full file could take approximately
**8-10 hours** for the ELT stage alone. This was judged impractical to
run to completion within the project timeline; the 2,000,000-row run
above is treated as sufficient large-scale evidence that the PySpark
loading path, the consistency equation, and the classification/quarantine
logic all hold correctly at meaningfully large scale.

**Suggested future improvement:** distribute the classification logic
itself (not just loading) across Spark using UDFs, or parallelize it with
Python's `multiprocessing`, since the quality rules in
`src/quality_rules.py` are already pure functions with no shared state -
making them straightforward to parallelize without any redesign.