# AI Prompts — Gold Layer

## Prompt 1: Gold Layer Aggregations

### PROMPT SENT

> Act as a **Senior Databricks Data Engineer**. Implement **ONLY the Gold layer** of my e-commerce Medallion pipeline using **PySpark/SQL + Delta Lake**.
>
> **Input:**
> - `silver.silver_customers`
> - `silver.silver_orders`
> - `silver.silver_products`
>
> **Create:**
>
> ```text
> src/gold/
> ├── 01_sales_by_product.sql
> ├── 02_revenue_by_customer.sql
> ├── 03_daily_weekly_trends.sql
> ├── 04_customer_segmentation.sql
> └── create_gold_tables.py
> ```
>
> **Gold tables:**
> 1. `gold.sales_by_product` — product_id, product_name, category, total_orders, total_revenue, avg_order_value
> 2. `gold.revenue_by_customer` — customer_id, customer_name, customer_segment, total_orders, total_revenue, avg_order_value, lifetime_value_actual
> 3. `gold.daily_weekly_trends` — order_date/week, total_orders, total_revenue, avg_order_value
> 4. `gold.customer_segmentation` — segment_type (High-Value, Repeat, One-Time, Inactive), customer_count, avg_revenue, total_revenue
>
> **Rules:** Silver only, no rejected rows, no Silver modifications, safe NULLs, no double-counting,
> correct SUM/COUNT/AVG, DECIMAL for money, explicit joins, Completed orders for revenue (document assumption).
>
> **Validation:** reconcile Gold totals against Silver. Orchestrate via `create_gold_tables.py`.
> Document purpose, formulas, segmentation rules, assumptions. Do not implement Dashboard.
>
> Also include this prompt in prompt history.

### AI RESPONSE SUMMARY

Implemented Gold layer:

- `src/gold/gold_common.py` — config, SQL loader, reconciliation validation
- `src/gold/01_sales_by_product.sql` through `04_customer_segmentation.sql`
- `src/gold/create_gold_tables.py` — orchestrator with post-run validation
- `src/gold/GOLD_LAYER.md` — documentation
- `tests/gold/test_gold_validation.py` — 6 aggregation/segmentation tests

**Segmentation rules (mutually exclusive):**

| Segment | Rule |
| ------- | ---- |
| Inactive | 0 Completed orders |
| One-Time | 1 Completed order |
| High-Value | 2+ Completed orders AND revenue ≥ $5,000 |
| Repeat | 2+ Completed orders AND revenue < $5,000 |

**Revenue assumption:** `order_status = 'Completed'` only.

### YOUR EVALUATION

**✓ What was good:**

- SQL files readable and maintainable; Python orchestrates execution order
- LEFT JOIN preserves zero-order customers and zero-sale products
- `COUNT(DISTINCT order_id)` prevents double-counting
- Built-in revenue reconciliation checks
- Behavioral segmentation with documented threshold
- Tests prove formulas on sample data

**✗ What needed fixing:**

- [Databricks cluster integration test pending]

**△ Assumptions:**

- High-Value threshold = $5,000 (env-configurable)
- `lifetime_value_actual` = sum of Completed `total_amount`
- Segmentation uses behavioral segments (not source Premium/Standard/Basic)

**FINAL DECISION:** Gold layer complete; Dashboard not implemented

---
