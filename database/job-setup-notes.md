# Full ETL Pipeline

End-to-end flow:

```text
Data Generation → Bronze → Silver → Gold → Dashboard Queries
```

**Entry point:** `src/run_full_etl_pipeline.py`

---

## Stages

| # | Stage | Script | Output |
| - | ----- | ------ | ------ |
| 1 | Data Generation | `generate_sample_data.py` | `customers.csv`, `orders.csv`, `products.csv` |
| 2 | Bronze Customers | `01_ingest_customers.py` | `bronze.bronze_customers` |
| 3 | Bronze Orders | `02_ingest_orders.py` | `bronze.bronze_orders` |
| 4 | Bronze Products | `03_ingest_products.py` | `bronze.bronze_products` |
| 5 | Silver Validate | `create_silver_tables.py` | `silver.silver_*`, `*_rejected`, `data_quality_report` |
| 6 | Gold Aggregate | `create_gold_tables.py` | `gold.sales_by_product`, etc. |
| 7 | Dashboard | `run_dashboard_queries.py` | Query results (temp views or `gold.dashboard_*`) |

---

## Run on Databricks (single command)

```python
import os
os.environ["BRONZE_INPUT_PATH"] = "/Volumes/main/default/raw_data"
os.environ["DASHBOARD_MATERIALIZE"] = "true"  # optional: persist dashboard tables

%run /Workspace/Repos/<user>/databricks-medallion-pipeline/src/run_full_etl_pipeline
```

Or as a **Databricks Job** — one Python task:

| Setting | Value |
| ------- | ----- |
| Python file | `src/run_full_etl_pipeline.py` |
| Env: `BRONZE_INPUT_PATH` | Your CSV/output directory |
| Env: `DASHBOARD_MATERIALIZE` | `true` (optional) |

---

## Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `BRONZE_INPUT_PATH` | Yes* | `data/` (local) | CSV folder for Bronze ingest |
| `ETL_DATA_OUTPUT_PATH` | No | `BRONZE_INPUT_PATH` | Where data generation writes CSVs |
| `SKIP_DATA_GENERATION` | No | `false` | Skip stage 1 if CSVs exist |
| `SKIP_DASHBOARD` | No | `false` | Skip stage 5 |
| `DASHBOARD_MATERIALIZE` | No | `false` | Write `gold.dashboard_*` Delta tables |
| `BRONZE_DATABASE` | No | `bronze` | Bronze schema |
| `SILVER_DATABASE` | No | `silver` | Silver schema |
| `GOLD_DATABASE` | No | `gold` | Gold schema |

\*Auto-set to repo `data/` when running locally without env vars.

---

## Multi-task Databricks Job (5 tasks)

Use `database/databricks_job.json`:

```text
data_generation
  → bronze_ingest_customers  ─┐
  → bronze_ingest_orders     ─┼→ silver_validate → gold_aggregate → dashboard_queries
  → bronze_ingest_products   ─┘
```

Bronze ingest tasks run **separately** (one CSV / one table each). Silver waits until all three Bronze tasks complete.

Update `git_url`, `git_branch`, and `BRONZE_INPUT_PATH` before creating the job.

---

## Medallion-only (skip data gen + dashboard)

Use `src/run_medallion_pipeline.py` for Bronze → Silver → Gold only.

---

## Verify

```sql
SELECT COUNT(*) FROM bronze.bronze_orders;
SELECT * FROM silver.data_quality_report LIMIT 5;
SELECT COUNT(*) FROM gold.sales_by_product;
SELECT * FROM gold.dashboard_kpi_summary;  -- if DASHBOARD_MATERIALIZE=true
```
