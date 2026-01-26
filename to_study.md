- **.option(fetchsize)** --> while reading from large datasets , specially from onprem.
  - (JDBC driver manages it internally. oracle creates a server side cursor for that query for N rows. once N rows are fetched, cursor moves forwared to mark the next set of rows to be picked by.
------------------
- in system.schema I see tables are "is insertable" as "NO" .. but then also I am able to insert rows into those tables.
------------------
- is it advisable to create stage table with `enableChangeDataFeed = True` property to make incremental pipeline or it's better to use watermark control table approach ?

```
ALTER TABLE main.fin_trans_fact
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```
