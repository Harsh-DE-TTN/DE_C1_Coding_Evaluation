# AI Prompts — Dashboard

## Prompt 1: Dashboard SQL Query Layer

### PROMPT SENT

> Act as a **Senior Databricks SQL/Data Analytics Engineer**.
>
> I have completed the Bronze → Silver → Gold pipeline. Now implement **ONLY the Dashboard query layer**.
>
> Use Gold tables:
> - `gold.sales_by_product`
> - `gold.revenue_by_customer`
> - `gold.daily_weekly_trends`
> - `gold.customer_segmentation`
>
> Create `src/dashboard/dashboard_queries.sql` with at least 3 SQL queries (preferably 4–5):
>
> 1. **Revenue by Product** — Bar chart Top products (product_name, category, total_orders, total_revenue)
> 2. **Revenue Trend** — Line chart (date/week, total_orders, total_revenue)
> 3. **Customer Segmentation** — Pie/Donut (segment_type, customer_count, total_revenue, avg_revenue)
> 4. **Revenue by Customer Segment** — Bar chart (customer_segment, customer_count, total_revenue, avg_order_value)
> 5. **KPI Summary** — KPI cards (total revenue, orders, AOV, customers, top product)
>
> Requirements: Gold only, optimized/readable SQL, NULL-safe, no duplicate counting,
> comments per visualization, Databricks SQL executable, ORDER BY/LIMIT where useful,
> no hard-coded results. Document query index at top. Validate against Gold schema.
> Do not create actual dashboard UI. Include prompt in .md file.

### AI RESPONSE SUMMARY

Created `src/dashboard/dashboard_queries.sql` with 5 queries (+ optional weekly trend variant):

| Query | Visualization | Source |
| ----- | ------------- | ------ |
| Top products by revenue | Bar chart | `sales_by_product` |
| Revenue trend daily | Line chart | `daily_weekly_trends` |
| Customer segmentation mix | Pie/Donut | `customer_segmentation` |
| Revenue by customer segment | Bar chart | `revenue_by_customer` |
| KPI summary | KPI cards | Multiple Gold tables |

Also added `src/dashboard/DASHBOARD_GUIDE.md` for usage notes.

### YOUR EVALUATION

**✓ What was good:**

- Gold-only queries with schema validation notes at file bottom
- Query index table documenting visualization and business question
- KPI uses daily trends for global revenue/orders (avoids double-count)
- Query 4 correctly uses `customer_segment` (Premium/Standard/Basic) vs Query 3 behavioral segments
- NULL-safe COALESCE throughout; LIMIT 10 for Top products

**△ Assumptions:**

- `gold` schema name (override catalog/schema in Databricks if different)
- KPI totals from `daily_weekly_trends` day grain, not sum of customer orders

**FINAL DECISION:** Dashboard SQL layer complete; UI not implemented

---
