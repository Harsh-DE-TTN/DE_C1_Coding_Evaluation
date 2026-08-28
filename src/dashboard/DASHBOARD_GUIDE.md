# Dashboard Query Layer

SQL queries for Databricks SQL Dashboard visualizations. Reads **Gold tables only**.

---

## File

`src/dashboard/dashboard_queries.sql`

---

## Query catalog

| # | Query | Visualization | Gold table(s) |
| - | ----- | ------------- | ------------- |
| 1 | Top products by revenue | Bar chart | `gold.sales_by_product` |
| 2 | Customer segmentation mix | Pie / Donut | `gold.customer_segmentation` |
| 3 | Revenue by customer segment | Bar chart | `gold.revenue_by_customer` |
| 4 | KPI summary | KPI cards | `gold.sales_by_product`, `gold.revenue_by_customer` |

---

## Assumptions

1. Gold metrics reflect **Completed orders only** (enforced in Gold layer).
2. Query 2 uses **behavioral** segments (High-Value, Repeat, One-Time, Inactive).
3. Query 3 uses **source** `customer_segment` (Premium, Standard, Basic) — a different dimension.
4. KPI totals aggregate from `gold.revenue_by_customer` (one row per customer).
5. No Bronze or Silver tables are queried from the dashboard layer.

---

## How to use in Databricks SQL

**Automated (recommended):** included in `src/run_full_etl_pipeline.py` stage 5, or run standalone:

```python
%run ./src/dashboard/run_dashboard_queries
```

Set `DASHBOARD_MATERIALIZE=true` to persist results as `gold.dashboard_*` tables.

**Manual:**

1. Open **SQL** → **SQL Editor** in Databricks workspace.
2. Copy one query block from `dashboard_queries.sql`.
3. Run against a SQL Warehouse with access to the `gold` schema.
4. In **Dashboards**, create a visualization and pin the query result.

---

## Related

- [data-quality-strategy.md](../../data-quality-strategy.md) — Gold revenue rules and segment logic
