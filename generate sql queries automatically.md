Generate the SQL programmatically (Notebook approach)

Use Python / Scala to auto-generate the COALESCE list instead of writing it by hand.

Python example (Databricks notebook):

```python
cols = ["col1","col2","col3"]  # fetch this dynamically if needed
exprs = [f"COALESCE(NULLIF(b.{c}, ''), a.{c}) AS {c}" for c in cols]
print(",\n".join(exprs))
```


➡ Paste the output into your SQL.
This is the industry-standard approach.

---------------------------------------------


**Yes — absolutely.**
This is a **very common and recommended Databricks pattern** 👍

You can **generate the full SELECT SQL in a Python cell and execute it** to create a **TEMP VIEW**.

---

## Minimal working pattern (Databricks)

### Python cell

```python
cols = ["col1", "col2", "col3"]  # can be generated dynamically

select_expr = ",\n".join(
    [f"COALESCE(NULLIF(b.{c}, ''), a.{c}) AS {c}" for c in cols]
)

sql_stmt = f"""
CREATE OR REPLACE TEMP VIEW ab_view AS
SELECT
  b.join_key,
  {select_expr}
FROM B b
LEFT JOIN A a
  ON b.join_key = a.join_key
"""

spark.sql(sql_stmt)
```

✅ Temp view `ab_view` is now created
✅ You can query it immediately:

```sql
SELECT * FROM ab_view LIMIT 10;
```

---

## Dynamic version (auto-read column list)

If A & B have same columns:

```python
cols = [
    r.col_name
    for r in spark.sql("DESCRIBE B").collect()
    if r.col_name not in ("join_key",)
]

select_expr = ",\n".join(
    [f"COALESCE(NULLIF(b.{c}, ''), a.{c}) AS {c}" for c in cols]
)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW ab_view AS
SELECT
  b.join_key,
  {select_expr}
FROM B b
LEFT JOIN A a
  ON b.join_key = a.join_key
""")
```

---


