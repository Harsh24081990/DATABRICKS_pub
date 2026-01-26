

---

## 1️⃣ Combined Control + Audit Table — DDL

```sql
CREATE TABLE IF NOT EXISTS stg.fin_trans_fact_ctrl (
  batch_id STRING,
  table_name STRING,
  last_process_date DATE,
  source_count BIGINT,
  stage_count BIGINT,
  main_merge_count BIGINT,
  status STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  error_msg STRING
)
USING DELTA;
```

---

## 2️⃣ Insert at Job Start (STATUS = STARTED)

Use this **once per run**, at the beginning.

```sql
INSERT INTO stg.fin_trans_fact_ctrl (
  batch_id,
  table_name,
  last_process_date,
  source_count,
  stage_count,
  main_merge_count,
  status,
  start_ts,
  end_ts,
  error_msg
)
VALUES (
  '${batch_id}',
  'fin_trans_fact',
  NULL,
  NULL,
  NULL,
  NULL,
  'STARTED',
  current_timestamp(),
  NULL,
  NULL
);
```

---

## 3️⃣ Update at Job Success (STATUS = SUCCESS)

Run this **after successful load & merge**.

```sql
UPDATE stg.fin_trans_fact_ctrl
SET last_process_date = '${last_process_date}'::DATE,
    source_count = ${source_count},
    stage_count = ${stage_count},
    main_merge_count = ${main_merge_count},
    status = 'SUCCESS',
    end_ts = current_timestamp()
WHERE batch_id = '${batch_id}'
  AND table_name = 'fin_trans_fact';
```

---

## 4️⃣ Update at Job Failure (STATUS = FAILED)

Use this inside a `try/except` block.

```sql
UPDATE stg.fin_trans_fact_ctrl
SET status = 'FAILED',
    error_msg = '${error_message}',
    end_ts = current_timestamp()
WHERE batch_id = '${batch_id}'
  AND table_name = 'fin_trans_fact';
```

---

## 5️⃣ Example Parameter Values (Python Side)

```python
params = {
    "batch_id": "20260126104530",
    "last_process_date": "2026-01-25",
    "source_count": 10500,
    "stage_count": 10500,
    "main_merge_count": 10500,
    "error_message": "NULL"
}
```


---

## 7️⃣ How to Query for Monitoring

### Latest Run Status

```sql
SELECT *
FROM stg.fin_trans_fact_ctrl
WHERE table_name = 'fin_trans_fact'
ORDER BY start_ts DESC
LIMIT 1;
```

### Failed Runs

```sql
SELECT *
FROM stg.fin_trans_fact_ctrl
WHERE status = 'FAILED'
ORDER BY start_ts DESC;
```

---
# ###########################################################################

Great question — this is **exactly where many pipelines stay half-done**.
Let’s make it **clear, practical, and copy-paste usable**.

---

## Core Idea (One Line)

> **You get the error message from Python when Spark throws an exception, and you store that message in the control table.**

Spark **does not automatically write errors to tables** — *you must catch them*.

---

## 1️⃣ Where the error message comes from

When something fails (SQL error, JDBC error, merge conflict, etc.), Spark throws an **exception**.

Example:

```text
AnalysisException: Table or view not found: main.balance_fact
```

This message is available in Python via `Exception as e`.

---

## 2️⃣ Correct Pattern: `try / except` (Required)

### ✅ This is the STANDARD production pattern

```python
try:
    # run pipeline steps
except Exception as e:
    # capture error message
```

---

## 3️⃣ Full Example (Clean & Minimal)

```python
batch_id = "20260126104530"

try:
    # Step 1: Run SQL / Spark jobs
    run_sql_file("sql/insert_incremental_to_stg.sql")
    run_sql_file("sql/transform_enrich.sql")
    run_sql_file("sql/merge_to_main.sql")

    # Step 2: Update control table as SUCCESS
    run_sql_file(
        "sql/update_ctrl_success.sql",
        {
            "batch_id": batch_id,
            "last_process_date": last_process_date,
            "source_count": source_count,
            "stage_count": stage_count,
            "main_merge_count": main_merge_count
        }
    )

except Exception as e:
    error_message = str(e)[:1000]   # truncate to avoid oversized strings

    # Step 3: Update control table as FAILED
    run_sql_file(
        "sql/update_ctrl_failed.sql",
        {
            "batch_id": batch_id,
            "error_message": error_message.replace("'", "''")
        }
    )

    raise   # VERY IMPORTANT: rethrow error
```

---

## 4️⃣ Why `str(e)` Works

```python
except Exception as e:
    print(str(e))
```

* `e` → exception object
* `str(e)` → human-readable error message
* Contains:

  * SQL syntax issues
  * Missing tables
  * Constraint violations
  * JDBC connection failures

---

## 5️⃣ Why truncate & escape the error message

### Truncate

Databricks errors can be **very long**.

```python
error_message = str(e)[:1000]
```

Prevents:

* SQL failures
* Oversized control-table rows

---

### Escape single quotes (IMPORTANT)

```python
error_message.replace("'", "''")
```

Because SQL strings use `'`.

---

## 6️⃣ Example Error Stored in Control Table

```text
status      = FAILED
error_msg   = AnalysisException: Table or view not found: main.balance_fact
start_ts   = 2026-01-26 10:45:30
end_ts     = 2026-01-26 10:45:31
```

This is exactly what ops teams expect.



---

## 8️⃣ Optional (Advanced but Useful)

### Capture Spark error class (Databricks)

```python
error_type = type(e).__name__
```

Store it if you want:

```sql
error_type STRING
```

---

## 9️⃣ Why re-raise the exception?

```python
raise
```

Because:

* Databricks job must **fail**
* Workflow retries depend on it
* Monitoring tools rely on job status

---



