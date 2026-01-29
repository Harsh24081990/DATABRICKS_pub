-- To handle null values:-
```sql
-- Returns the first NON NULL value. 
SELECT COALESCE('', 'A') AS result;
-- to handle empty string as well
COALESCE(NULLIF(b.document_key, ''), a.document_key) AS document_key
```
- You can pass any number of columns/expressions to COALESCE; it returns the first non-NULL value from left to right.

- can use case also if more validations needed.
```sql
CASE
  WHEN b.document_key IS NOT NULL THEN b.document_key
  ELSE a.document_key
END AS document_key
```

```sql
COALESCE(
  NULLIF(
    CASE
      WHEN b.document_key IN ('', 'UNKNOWN', 'NA', '0') THEN NULL
      ELSE b.document_key
    END,
    NULL
  ),
  a.document_key
) AS document_key
```

-----------


## Databricks SQL – create TEMP VIEW (syntax)

```sql
CREATE OR REPLACE TEMP VIEW ab_view AS
SELECT
  COALESCE(NULLIF(b.document_key, ''), a.document_key) AS document_key,
  b.col1,
  b.col2,
  ...
FROM B b
LEFT JOIN A a
  ON b.join_key = a.join_key;
```

> TEMP VIEW is session-scoped (auto-dropped).

---

## MERGE using the TEMP VIEW

```sql
MERGE INTO C c
USING ab_view s
ON c.document_key = s.document_key
WHEN MATCHED THEN
  UPDATE SET
    c.col1 = s.col1,
    c.col2 = s.col2
WHEN NOT MATCHED THEN
  INSERT (document_key, col1, col2)
  VALUES (s.document_key, s.col1, s.col2);
```

---






