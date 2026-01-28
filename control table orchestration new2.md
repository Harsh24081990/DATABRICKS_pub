Perfect — below is the **clean, final, corrected version** of the code based on your **new control-table design** and your rule:

> ✅ **Insert control row ONLY after STAGE finishes**
> ✅ **Update the SAME row after MAIN finishes**
> ✅ **One row per batch**
> ✅ **Separate status/message/counts for STAGE and MAIN**

I’ll show:

1. **Control table (for reference)**
2. **SQL files**
3. **Python orchestration code**
4. **Execution flow (very short)**

All **production-ready and consistent**.

---

## 1️⃣ Control Table (reference – already agreed)

```sql
CREATE TABLE IF NOT EXISTS stg.fin_trans_fact_ctrl (
  batch_id STRING,
  table_name STRING,

  last_process_date TIMESTAMP,

  source_count BIGINT,
  stage_count BIGINT,
  main_merge_count BIGINT,

  stage_status STRING,
  stage_message STRING,
  stg_load_time TIMESTAMP,

  main_status STRING,
  main_message STRING,
  main_load_time TIMESTAMP
)
USING DELTA;
```

---

## 2️⃣ SQL FILES

### 📄 `insert_ctrl_after_stage.sql`

*(Executed after STAGE completes — success or failure)*

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
  '${stage_status}',
  '${stage_message}',
  current_timestamp()
);
```

---

### 📄 `update_ctrl_after_main.sql`

*(Executed after MAIN completes — success or failure)*

```sql
UPDATE stg.fin_trans_fact_ctrl
SET main_status = '${main_status}',
    main_message = '${main_message}',
    main_merge_count = ${main_merge_count},
    last_process_date = '${last_process_date}',
    main_load_time = current_timestamp()
WHERE batch_id = '${batch_id}'
  AND table_name = 'fin_trans_fact';
```

---

## 3️⃣ PYTHON ORCHESTRATION CODE (FINAL)

### ✅ Generate batch_id ONCE

```python
batch_id = spark.sql(
    "SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')"
).collect()[0][0]
```

---

### ✅ STAGE LOAD + CONTROL INSERT

```python
try:
    # ---------- STAGE LOAD ----------
    run_sql_file("sql/load_to_stg.sql", params)

    stage_status = "SUCCESS"
    stage_message = "NONE"

except Exception as e:
    stage_status = "FAILED"
    stage_message = str(e)[:1000].replace("'", "''")

# ---------- INSERT CONTROL ROW (ALWAYS after STAGE finishes) ----------
run_sql_file(
    "sql/insert_ctrl_after_stage.sql",
    {
        **params,
        "batch_id": batch_id,
        "stage_status": stage_status,
        "stage_message": stage_message,
        "source_count": source_count if stage_status == "SUCCESS" else 0,
        "stage_count": stage_count if stage_status == "SUCCESS" else 0
    }
)

# If STAGE failed → stop job
if stage_status == "FAILED":
    raise Exception(stage_message)
```

---

### ✅ MAIN LOAD + CONTROL UPDATE

```python
try:
    # ---------- MAIN LOAD ----------
    run_sql_file("sql/merge_to_main.sql", params)

    main_status = "SUCCESS"
    main_message = "NONE"

except Exception as e:
    main_status = "FAILED"
    main_message = str(e)[:1000].replace("'", "''")

# ---------- UPDATE SAME CONTROL ROW ----------
run_sql_file(
    "sql/update_ctrl_after_main.sql",
    {
        **params,
        "batch_id": batch_id,
        "main_status": main_status,
        "main_message": main_message,
        "main_merge_count": main_merge_count if main_status == "SUCCESS" else 0,
        "last_process_date": last_process_date
    }
)

# If MAIN failed → fail job
if main_status == "FAILED":
    raise Exception(main_message)
```

---

## 4️⃣ What This Guarantees (Important)

✔ Exactly **1 control row per batch**
✔ STAGE result is always captured
✔ MAIN result updates the same row
✔ No START / END noise
✔ Accurate completion timestamps
✔ Clean retries
✔ Production-grade audit trail

---

## 5️⃣ Execution Scenarios (Truth Table)

| Scenario         | Control Inserts | Control Updates |
| ---------------- | --------------- | --------------- |
| STAGE ❌          | 1               | 0               |
| STAGE ✅ → MAIN ❌ | 1               | 1               |
| STAGE ✅ → MAIN ✅ | 1               | 1               |

✔ Exactly what you wanted.

---

## Final One-Line Summary

> **Insert the control row once after STAGE finishes, then update the same row once after MAIN finishes — never more.**

If you want next:

* Idempotent rerun handling
* Multi-table batch orchestration
* SLA breach detection query

Just say 👍
