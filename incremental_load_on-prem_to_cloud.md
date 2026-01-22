Got it. Below is a **clean, simple, production-style design** that matches your constraints **exactly**:

✅ **Oracle read using DataFrames (JDBC)**
✅ **ALL SQL stored in external `.sql` files**
✅ **Same table name `fin_trans_fact`**
✅ **Schemas: `stg` and `main`**
✅ **Control table in `stg`**
✅ **Incremental → transform → load**

This pattern is very common in Databricks jobs.

---

# 1️⃣ Overall Design

```
Oracle.fin_trans_fact
        ↓ (PySpark DataFrame – JDBC incremental read)
stg.fin_trans_fact
        ↓ (SQL transformation using balance table)
main.fin_trans_fact
        ↓
stg.fin_trans_fact_ctrl
```

---

# 2️⃣ SQL Files Structure

```
sql/
 ├── create_schemas.sql
 ├── create_control_table.sql
 ├── insert_incremental_to_stg.sql
 ├── transform_enrich.sql
 ├── merge_to_main.sql
 └── update_control_table.sql
```

All SQL is parameterized and executed from PySpark.

---

# 3️⃣ SQL Files Content

## `create_schemas.sql`

```sql
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS main;
```

---

## `create_control_table.sql`

```sql
CREATE TABLE IF NOT EXISTS stg.fin_trans_fact_ctrl (
  table_name STRING,
  last_process_date DATE,
  updated_ts TIMESTAMP
)
USING DELTA;
```

---

## `insert_incremental_to_stg.sql`

```sql
INSERT INTO stg.fin_trans_fact
SELECT *
FROM fin_trans_fact_inc;
```

> `fin_trans_fact_inc` will be a **temporary view** created from DataFrame.

---

## `transform_enrich.sql`

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
  ON f.account_id = b.account_id;
```

---

## `merge_to_main.sql`

```sql
MERGE INTO main.fin_trans_fact tgt
USING fin_trans_fact_enriched src
ON tgt.trans_id = src.trans_id

WHEN MATCHED THEN
  UPDATE SET
    tgt.amount = src.amount,
    tgt.current_balance = src.current_balance,
    tgt.balance_type = src.balance_type,
    tgt.process_date = src.process_date

WHEN NOT MATCHED THEN
  INSERT (
    trans_id,
    account_id,
    amount,
    process_date,
    current_balance,
    balance_type
  )
  VALUES (
    src.trans_id,
    src.account_id,
    src.amount,
    src.process_date,
    src.current_balance,
    src.balance_type
  );
```

---

## `update_control_table.sql`

```sql
UPDATE stg.fin_trans_fact_ctrl
SET last_process_date = DATE '${max_process_date}',
    updated_ts = current_timestamp()
WHERE table_name = 'fin_trans_fact';
```

---

# 4️⃣ PySpark Driver Code (Main Notebook / Job)

## Utility: Execute SQL File

```python
def run_sql_file(file_path: str, params: dict = None):
    with open(file_path, "r") as f:
        sql_text = f.read()

    if params:
        for k, v in params.items():
            sql_text = sql_text.replace(f"${{{k}}}", str(v))

    spark.sql(sql_text)
```

---

## Step 1: Create Schemas & Control Table

```python
run_sql_file("sql/create_schemas.sql")
run_sql_file("sql/create_control_table.sql")
```

---

## Step 2: Read Watermark (SQL → DF)

```python
last_process_date = (
    spark.sql("""
      SELECT last_process_date
      FROM stg.fin_trans_fact_ctrl
      WHERE table_name = 'fin_trans_fact'
    """)
    .collect()[0][0]
)
```

---

## Step 3: Incremental Read from Oracle (DataFrame)

```python
oracle_df = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:oracle:thin:@//host:port/service")
    .option("user", "oracle_user")
    .option("password", "oracle_password")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .option(
        "dbtable",
        f"""
        ( SELECT *
          FROM fin_trans_fact
          WHERE process_date > DATE '{last_process_date}'
        )
        """
    )
    .load()
)
```

---

## Step 4: Create Temp View for SQL

```python
oracle_df.createOrReplaceTempView("fin_trans_fact_inc")
```

---

## Step 5: Load Incremental Data into `stg.fin_trans_fact`

```python
run_sql_file("sql/insert_incremental_to_stg.sql")
```

---

## Step 6: Transformation / Enrichment

```python
run_sql_file("sql/transform_enrich.sql")
```

---

## Step 7: Merge into Main Table

```python
run_sql_file("sql/merge_to_main.sql")
```

---

## Step 8: Update Control Table

```python
max_process_date = (
    oracle_df
    .selectExpr("max(process_date) as max_dt")
    .collect()[0]["max_dt"]
)

run_sql_file(
    "sql/update_control_table.sql",
    {"max_process_date": max_process_date}
)
```

---

# 5️⃣ Why This Pattern Is Strong

✔ Clean separation: **Python = orchestration, SQL = logic**
✔ Easy to maintain SQL independently
✔ Same table name preserved (`fin_trans_fact`)
✔ Scales well for large Oracle tables
✔ Databricks-job & CI/CD friendly

---

## Optional Improvements (Later)

* Add `batch_id` in stage
* Deduplicate stage data before merge
* Add record-count audits
* Partition `stg.fin_trans_fact` by `process_date`

If you want, I can:

* Convert this into a **Databricks Workflow**
* Add **error handling & retries**
* Add **CDC logic**
* Make it **idempotent**

Just say the word 👍

