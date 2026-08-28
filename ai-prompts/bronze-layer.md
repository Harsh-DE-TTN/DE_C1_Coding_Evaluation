# AI Prompts — Bronze Layer

## Prompt 1: Bronze Layer Ingestion

### PROMPT SENT

> Act as a Senior Databricks Data Engineer specializing in PySpark, Delta Lake, and Medallion Architecture.
>
> I have already generated these source CSV files:
>
> ```text
> data/
> ├── customers.csv
> ├── orders.csv
> └── products.csv
> ```
>
> Now implement ONLY the Bronze Layer ingestion for all three tables.
> Do not implement Silver, Gold, Dashboard, or data-quality transformations yet.
>
> **Bronze objective:** CSV → Bronze Ingestion → Bronze Delta Tables. Preserve raw data.
> Do NOT: remove duplicates, NULLs, fix FKs, clean values, apply business rules, filter bad records.
>
> **Required files:**
>
> ```text
> src/bronze/
> ├── 01_ingest_customers.py
> ├── 02_ingest_orders.py
> ├── 03_ingest_products.py
> └── ingest_all.py
> ```
>
> **Input schemas:** explicit PySpark schemas with documented DECIMAL precision.
>
> **Bronze tables:** `bronze.bronze_customers`, `bronze.bronze_orders`, `bronze.bronze_products`
>
> **Metadata:** `_ingestion_timestamp`, `_source_file`, optionally `_ingestion_date`
>
> **Ingestion logic:** validate file exists, read CSV, preserve NULLs/duplicates/invalid FKs,
> add metadata, write Delta, verify write, report counts. Fail on missing file.
>
> **Path config:** centralized INPUT_PATH, BRONZE_DATABASE, file names — adaptable to
> DBFS / S3 / Volumes.
>
> **ingest_all.py:** customers → orders → products with START/SUCCESS/FAILED logging.
>
> **Post-ingestion validation:** row counts, schema, preserved intentional defects
> (NULL emails, dup IDs, NULL/invalid FKs).
>
> **Documentation:** `src/bronze/BRONZE_LAYER.md`
>
> Also save this prompt in md files and check all previous prompts are saved.

### AI RESPONSE SUMMARY

Implemented:

- `src/bronze/bronze_common.py` — config, schemas, ingest, validation
- `src/bronze/01_ingest_customers.py`
- `src/bronze/02_ingest_orders.py`
- `src/bronze/03_ingest_products.py`
- `src/bronze/ingest_all.py`
- `src/bronze/BRONZE_LAYER.md`

Metadata: `_ingestion_timestamp`, `_ingestion_date`, `_source_file`

Initially included local PySpark + delta-spark support for local runs.

### YOUR EVALUATION

**✓ What was good:**

- Centralized configuration (env-var overrides)
- Explicit DECIMAL schemas
- Raw defect preservation validation
- Modular reusable `bronze_common.py`
- `ingest_all.py` stops on first failure

**△ Assumptions:**

- `BRONZE_WRITE_MODE=overwrite` default for reload
- Databricks cluster provides Spark + Delta

---

## Prompt 2: Remove Local Run Files

### PROMPT SENT

> remove all files for local run

### AI RESPONSE SUMMARY

Removed local-run support:

- Deleted `LOCAL_WAREHOUSE_DIR`, `configure_spark_with_delta_pip`, repo-root CSV default
- Removed `pyspark` and `delta-spark` from `requirements.txt`
- Updated `BRONZE_LAYER.md` — Databricks-only instructions
- `BRONZE_INPUT_PATH` now required (Volume/DBFS/cloud path)
- `get_spark()` uses `SparkSession.builder.getOrCreate()` on cluster

### YOUR EVALUATION

**✓ ACCEPTED** — Bronze is Databricks-only; no local Spark artifacts in repo

**FINAL DECISION:** Bronze scripts ready for Databricks cluster/job execution

---

## Prompt 3: Add All Prompt History

### PROMPT SENT

> add all prompt history in .md files

### AI RESPONSE SUMMARY

Updated all `ai-prompts/*.md` files and created `PROMPT_HISTORY.md` master index.

### YOUR EVALUATION

**✓ ACCEPTED**

---
