# Design Notes

## Architecture Overview

Medallion architecture for e-commerce analytics:

```text
CSV (data/)  →  Bronze (raw Delta)  →  Silver (validate + quarantine)  →  Gold (aggregations)  →  Dashboard SQL
```

**Principles:**

- Bronze is immutable raw landing — source columns preserved plus ingest metadata (`_ingestion_timestamp`, `_ingestion_date`, `_source_file`).
- Silver applies quality checks, splits PASS/FAIL, and writes `data_quality_report`. Bronze is never modified.
- Gold reads only validated Silver PASS tables and produces business-ready aggregates.
- Dashboard consumes Gold tables via SQL only (no UI framework).

See [data-model.md](data-model.md) for entity relationships.

## Data Model & Schema

Three source entities with FK relationships:

| Entity | PK | FK | Notes |
| ------ | -- | -- | ----- |
| customers | `customer_id` | — | 10,000 rows; segment, LTV |
| products | `product_id` | — | 500 rows; price, cost, stock |
| orders | `order_id` | `customer_id`, `product_id` | 100,000 rows; revenue driver |

**Processing order:** customers → products → orders (Silver FK checks require valid parents first).

## Bronze Layer Design

- **Bronze:** one script per CSV — `01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py` (`ingest_all.py` = shared utilities only)

## Silver Layer Design

- **Location:** `src/silver/` (shared utilities in `01_quality_completeness.py`)
- **Modules:** completeness, uniqueness, type validation, referential integrity, business logic
- **Output tables:**
  - `silver.silver_*` — PASS rows
  - `silver.silver_*_rejected` — FAIL rows with `quality_status`, `quality_check_result`, `quality_check_reason`
  - `silver.data_quality_report` — per-check metrics
- **Duplicate key policy:** all rows sharing a duplicate key are rejected (no survivor selection)
- **FK validation:** orders checked against valid Silver customers/products, not Bronze

## Gold Layer Design

- **Location:** `src/gold/` (shared utilities in `create_gold_tables.py`)
- **Four aggregations:**
  1. `sales_by_product` — revenue and order counts per product
  2. `revenue_by_customer` — spend per customer with segment
  3. `daily_weekly_trends` — day/week rollups
  4. `customer_segmentation` — High-Value / Repeat / One-Time / Inactive
- **Revenue rule:** only `order_status = 'Completed'` orders count toward revenue
- **High-Value threshold:** `$5,000` total revenue (configurable via env)

## Data Quality Validation Strategy

460 intentional defects in seed CSVs (see `DATA_GENERATION_NOTES.md`):

| Issue | Count |
| ----- | ----: |
| NULL email | 50 |
| Duplicate customer_id | 10 |
| NULL order customer_id | 100 |
| NULL order product_id | 200 |
| Invalid customer_id | 50 |
| Invalid product_id | 30 |
| Duplicate order_id | 20 |

Silver thresholds documented in [data-quality-strategy.md](data-quality-strategy.md).

## Debugging Approach

1. **Local first:** data generation validation + pytest for Silver/Gold logic
2. **Databricks:** run layers sequentially; inspect `data_quality_report` after Silver
3. **Row-count reconciliation:** Bronze total = Silver PASS + Silver rejected per dataset
4. **Known issues:** local Delta classpath failures → resolved by Databricks-only Bronze; ~700 vs 460 injection count clarified in docs

See [debugging-notes.md](debugging-notes.md) and [ai-prompts/debugging.md](ai-prompts/debugging.md).
