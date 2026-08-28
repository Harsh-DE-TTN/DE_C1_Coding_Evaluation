# Database Setup Notes

## Purpose

Provision Databricks schemas (`bronze`, `silver`, `gold`) before running the Medallion pipeline.

## Setup steps

### 1. Run schema script

Execute `database/schema.sql` in **Databricks SQL** or a notebook:

```sql
%run ./database/schema.sql
```

Or paste the `CREATE DATABASE` statements into the SQL Editor.

### 2. Configure permissions (Unity Catalog)

Grant the pipeline service principal or user:

| Schema | Permission |
| ------ | ---------- |
| `bronze` | `CREATE TABLE`, `MODIFY`, `SELECT` |
| `silver` | `CREATE TABLE`, `MODIFY`, `SELECT` |
| `gold` | `CREATE TABLE`, `MODIFY`, `SELECT` |

Dashboard consumers need `SELECT` on `gold` only.

### 3. Upload seed CSVs

Copy `data/*.csv` to the Bronze input path:

```text
/Volumes/<catalog>/<schema>/<volume>/data/customers.csv
/Volumes/<catalog>/<schema>/<volume>/data/orders.csv
/Volumes/<catalog>/<schema>/<volume>/data/products.csv
```

Set `BRONZE_INPUT_PATH` to the directory containing these files.

### 4. Run pipeline

```text
ingest_all.py  →  create_silver_tables.py  →  create_gold_tables.py
```

### 5. Verify

```sql
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
SHOW TABLES IN gold;
SELECT * FROM silver.data_quality_report LIMIT 20;
```

## Notes

- Tables are **Delta format**, created by PySpark `saveAsTable` — not pre-created manually.
- `schema.sql` documents expected table names and columns; actual DDL is applied at runtime with `overwriteSchema=true`.
- Default write mode is `overwrite` per layer run (configurable via env vars).

## Environment variables

| Variable | Default |
| -------- | ------- |
| `BRONZE_DATABASE` | `bronze` |
| `SILVER_DATABASE` | `silver` |
| `GOLD_DATABASE` | `gold` |
| `BRONZE_INPUT_PATH` | *(required on Databricks)* |
