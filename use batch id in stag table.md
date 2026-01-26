Nice — this gives you **full traceability** with very little extra work.
Below are **only the modified / new queries** you need when `batch_id` is added to the **staging table**.

Everything is **copy-paste ready**.

---

## 1️⃣ Modify STG Table (Add batch_id)

```sql
ALTER TABLE stg.fin_trans_fact
ADD COLUMNS (
  batch_id STRING,
  load_ts TIMESTAMP
);
```

> `load_ts` is optional but **highly recommended**.

---

## 2️⃣ Incremental Insert into STG (WITH batch_id)

### 🔁 Replace your existing STG insert SQL with this

```sql
INSERT INTO stg.fin_trans_fact
SELECT
  f.*,
  '${batch_id}'     AS batch_id,
  current_timestamp() AS load_ts
FROM fin_trans_fact_inc f;
```

> `fin_trans_fact_inc` = temp view created from Oracle DataFrame

---

## 3️⃣ Transformation Query (Batch-aware)

### 🔁 Replace transform SQL

```sql
CREATE OR REPLACE TEMP VIEW fin_trans_fact_enriched AS
SELECT
    f.trans_id,
    f.account_id,
    f.amount,
    f.process_date,
    b.current_balance,
    b.balance_type
FROM stg.fin_trans_fact f
LEFT JOIN main.balance_fact b
  ON f.account_id = b.account_id
WHERE f.batch_id = '${batch_id}';
```

✔ Ensures **only current batch** is processed
✔ Prevents accidental reprocessing of old STG data

---

## 4️⃣ Stage Record Count (Batch-specific)

```sql
SELECT COUNT(*) AS stage_count
FROM stg.fin_trans_fact
WHERE batch_id = '${batch_id}';
```

---

## 5️⃣ Control + Audit Table (UNCHANGED)

No change needed — `batch_id` already exists there.

---

## 6️⃣ Optional: Re-run Cleanup (Very Useful)

If a batch fails and you want to rerun:

```sql
DELETE FROM stg.fin_trans_fact
WHERE batch_id = '${batch_id}';
```

Then restart the job safely.

---

## 7️⃣ Optional: Retention Cleanup

```sql
DELETE FROM stg.fin_trans_fact
WHERE load_ts < current_date() - INTERVAL 30 DAYS;
```

---

## 8️⃣ Updated End-to-End Flow

```
Oracle.fin_trans_fact
        ↓
Temp View fin_trans_fact_inc
        ↓ (+ batch_id)
stg.fin_trans_fact
        ↓ (filter by batch_id)
fin_trans_fact_enriched
        ↓
main.fin_trans_fact
        ↓
stg.fin_trans_fact_ctrl
```

---

## 9️⃣ Why This Is the Right Design

* Full **row-level traceability**
* Easy **reruns & rollback**
* Clean **batch isolation**
* Audits become trustworthy
* Production-grade pattern

---

### One-Line Recommendation

> **Once you add `batch_id` to STG, always filter transformations by `batch_id`.**

If you want next:

* Idempotent MERGE logic
* Handling late-arriving data
* Multi-table batch orchestration

Just say 👍
