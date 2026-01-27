Perfect — this is a **more mature, automation-friendly design** 👍
You’re saying:

* ❌ No “STARTED” rows
* ✅ Control table updated **only after STAGE finishes** and **after MAIN finishes**
* ✅ Status/message should be **derived automatically**, not hardcoded
* ✅ Only store **completion timestamps**

That’s a **clean audit-by-result** pattern.

Below is a **clear design + exact SQL you can copy-paste**.

---

## 1️⃣ Revised Control Table Structure (Final)

```sql
DROP TABLE IF EXISTS stg.fin_trans_fact_ctrl;
```

```sql
CREATE TABLE IF NOT EXISTS stg.fin_trans_fact_ctrl (
  batch_id STRING,
  table_name STRING,

  -- watermark
  last_process_date TIMESTAMP,

  -- counts
  source_count BIGINT,
  stage_count BIGINT,
  main_merge_count BIGINT,

  -- stage audit
  stage_status STRING,        -- SUCCESS / FAILED
  stage_message STRING,       -- NONE or error
  stg_load_time TIMESTAMP,    -- completion time

  -- main audit
  main_status STRING,         -- SUCCESS / FAILED
  main_message STRING,        -- NONE or error
  main_load_time TIMESTAMP    -- completion time
)
USING DELTA;
```

---

## 2️⃣ How “automatic status & message” works (Important)

We **do not** rely on SQL returning status.
Instead:

* If a query **finishes without exception** → `SUCCESS`
* If Spark **throws exception** → `FAILED`, capture error message

This is the **only reliable way** in Spark.

---

## 3️⃣ STAGE Load → Control Table Update (Automatic)

### ✅ After STAGE load finishes successfully

```sql
INSERT INTO stg.fin_trans_fact_ctrl (
  batch_id,
  table_name,
  source_count,
  stage_count,
  stage_status,
  stage_message,
  stg_load_time
)
VALUES (
  '${batch_id}',
  'fin_trans_fact',
  ${source_count},
  ${stage_count},
  'SUCCESS',
  'NONE',
  current_timestamp()
);
```

---

### ❌ If STAGE load fails (Python side)

```sql
INSERT INTO stg.fin_trans_fact_ctrl (
  batch_id,
  table_name,
  stage_status,
  stage_message,
  stg_load_time
)
VALUES (
  '${batch_id}',
  'fin_trans_fact',
  'FAILED',
  '${error_message}',
  current_timestamp()
);
```

➡ Job **stops here**, MAIN never runs.

---

## 4️⃣ MAIN Load → Control Table Update (Automatic)

### ✅ After MAIN load finishes successfully

```sql
UPDATE stg.fin_trans_fact_ctrl
SET main_status = 'SUCCESS',
    main_message = 'NONE',
    main_merge_count = ${main_merge_count},
    last_process_date = '${last_process_date}',
    main_load_time = current_timestamp()
WHERE batch_id = '${batch_id}'
  AND table_name = 'fin_trans_fact';
```

---

### ❌ If MAIN load fails

```sql
UPDATE stg.fin_trans_fact_ctrl
SET main_status = 'FAILED',
    main_message = '${error_message}',
    main_load_time = current_timestamp()
WHERE batch_id = '${batch_id}'
  AND table_name = 'fin_trans_fact';
```

---

## 5️⃣ Python Orchestration Pattern (Key Logic)

This is where “automatic” actually happens 👇

```python
try:
    # STAGE load
    run_sql_file("sql/load_to_stg.sql", params)
    stage_status = "SUCCESS"
    stage_message = "NONE"
except Exception as e:
    stage_status = "FAILED"
    stage_message = str(e)[:1000].replace("'", "''")

    run_sql_file(
        "sql/insert_ctrl_stage.sql",
        {**params, "error_message": stage_message}
    )
    raise
```

Then MAIN:

```python
try:
    run_sql_file("sql/merge_to_main.sql", params)
    main_status = "SUCCESS"
    main_message = "NONE"
except Exception as e:
    main_status = "FAILED"
    main_message = str(e)[:1000].replace("'", "''")

    run_sql_file(
        "sql/update_ctrl_main_failed.sql",
        {**params, "error_message": main_message}
    )
    raise
```

---

## 6️⃣ Why This Design Is Correct

✔ Control table reflects **actual outcomes**, not intent
✔ No misleading “STARTED” states
✔ Completion timestamps are **accurate**
✔ Partial success is visible
✔ Works cleanly with retries
✔ Easy to query operationally

---

## 7️⃣ Example Final Control Row (Success)

| column         | value               |
| -------------- | ------------------- |
| batch_id       | 20260126120000      |
| stage_status   | SUCCESS             |
| stage_message  | NONE                |
| stg_load_time  | 2026-01-26 12:01:10 |
| main_status    | SUCCESS             |
| main_message   | NONE                |
| main_load_time | 2026-01-26 12:02:05 |

---

## 8️⃣ Example Failure Case (Stage Failed)

| column        | value                      |
| ------------- | -------------------------- |
| stage_status  | FAILED                     |
| stage_message | ORA-00942: table not found |
| main_status   | NULL                       |
| main_message  | NULL                       |

---

## One-Line Final Takeaway

> **In Spark, “automatic status” means: success = no exception, failure = exception — and timestamps should always represent completion, not start.**

If you want next, I can:

* Add **derived overall_status view**
* Add **idempotent upsert logic**
* Add **retry-aware audit rows**

Just say 👍
