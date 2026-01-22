Below is a **minimal, clean extension** to your existing design that adds
✅ **batch_id in stage**
✅ **record-count audits**
without changing the overall flow.

---

## 1️⃣ Why batch_id only in STG (quick rationale)

* Stage is **operational** → batch tracking needed
* Main is **business-curated** → batch_id usually not required
* Keeps main table clean and stable

---

## 2️⃣ Changes to STG Table

```sql
ALTER TABLE stg.fin_trans_fact
ADD COLUMNS (
  batch_id STRING,
  load_ts TIMESTAMP
);
```

---

## 3️⃣ Audit Table (STG)

```sql
CREATE TABLE IF NOT EXISTS stg.fin_trans_fact_audit (
  batch_id STRING,
  source_count BIGINT,
  stage_count BIGINT,
  main_merge_count BIGINT,
  status STRING,
  audit_ts TIMESTAMP
)
USING DELTA;
```

---

## 4️⃣ Generate batch_id (PySpark – orchestration only)

```python
from datetime import datetime

batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
```

---

## 5️⃣ Add batch_id to Incremental Data (DF → Temp View)

```python
from pyspark.sql.functions import lit, current_timestamp

oracle_df_with_batch = (
    oracle_df
    .withColumn("batch_id", lit(batch_id))
    .withColumn("load_ts", current_timestamp())
)

oracle_df_with_batch.createOrReplaceTempView("fin_trans_fact_inc")
```

---

## 6️⃣ Load to STG (SQL file stays the same)

```sql
INSERT INTO stg.fin_trans_fact
SELECT *
FROM fin_trans_fact_inc;
```

---

## 7️⃣ Record-Count Audits

### Source Count (Oracle DF)

```python
source_count = oracle_df.count()
```

---

### Stage Count (SQL)

```python
stage_count = (
    spark.sql(f"""
      SELECT COUNT(*)
      FROM stg.fin_trans_fact
      WHERE batch_id = '{batch_id}'
    """)
    .collect()[0][0]
)
```

---

### Merge Count (affected rows)

```python
main_merge_count = (
    spark.sql("""
      SELECT COUNT(*)
      FROM fin_trans_fact_enriched
    """)
    .collect()[0][0]
)
```

---

## 8️⃣ Insert Audit Record

```python
spark.sql(f"""
  INSERT INTO stg.fin_trans_fact_audit
  VALUES (
    '{batch_id}',
    {source_count},
    {stage_count},
    {main_merge_count},
    'SUCCESS',
    current_timestamp()
  )
""")
```

---

## 9️⃣ Updated Flow (with batch & audit)

```
Oracle.fin_trans_fact
        ↓ (incremental)
stg.fin_trans_fact (batch_id, load_ts)
        ↓
audit counts captured
        ↓
transform + merge
        ↓
main.fin_trans_fact
        ↓
audit logged
```

---

## 10️⃣ What This Gives You (Short)

* 🔁 Safe reruns (`DELETE WHERE batch_id = ?`)
* 📊 Source ↔ Stage ↔ Main reconciliation
* 🧪 Easy failure analysis
* 🛠 Production-grade observability

---

If you want next:

* **Auto-rollback on count mismatch**
* **Duplicate prevention using batch_id**
* **Databricks Workflow integration**

Just say 👍
