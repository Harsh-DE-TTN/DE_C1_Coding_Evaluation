-- =============================================================================
-- E-commerce Dashboard SQL Queries (Databricks SQL)
-- =============================================================================
-- Source: Gold layer ONLY (no Bronze/Silver queries).
-- Assumption: Gold revenue metrics already reflect Completed orders only.
--
-- Query Index
-- -----------------------------------------------------------------------------
-- | # | Query                         | Visualization        | Business Question                          | Gold Table(s)                    |
-- |---|-------------------------------|----------------------|--------------------------------------------|----------------------------------|
-- | 1 | top_products_by_revenue       | Bar chart            | Which products drive the most revenue?     | sales_by_product                 |
-- | 2 | revenue_trend_daily            | Line chart           | How is revenue trending over time?         | daily_weekly_trends              |
-- | 3 | customer_segmentation_mix      | Pie / Donut chart    | How are customers split by behavior?       | customer_segmentation            |
-- | 4 | revenue_by_customer_segment  | Bar chart            | How does revenue vary by customer tier?    | revenue_by_customer              |
-- | 5 | kpi_summary                    | KPI cards            | What are overall business health metrics?  | sales_by_product, revenue_by_customer, daily_weekly_trends |
-- =============================================================================


-- =============================================================================
-- QUERY 1: Top Products by Revenue
-- Visualization : Bar chart (horizontal or vertical)
-- Question      : Which products generate the highest completed-order revenue?
-- Gold table(s) : gold.sales_by_product
-- Notes         : Excludes products with zero revenue via WHERE; LIMIT for Top N chart.
-- =============================================================================

SELECT
    product_name,
    category,
    COALESCE(total_orders, 0) AS total_orders,
    COALESCE(total_revenue, CAST(0 AS DECIMAL(14, 2))) AS total_revenue
FROM gold.sales_by_product
WHERE COALESCE(total_revenue, 0) > 0
ORDER BY total_revenue DESC, product_name ASC
LIMIT 10;


-- =============================================================================
-- QUERY 2: Revenue Trend Over Time
-- Visualization : Line chart (x = date, y = revenue); optional second series for orders
-- Question      : How do daily revenue and order volume change over time?
-- Gold table(s) : gold.daily_weekly_trends
-- Notes         : Filter period_type = 'day' to avoid mixing grains.
--                 For weekly line chart, change filter to period_type = 'week'.
-- =============================================================================

SELECT
    period_start AS trend_date,
    COALESCE(total_orders, 0) AS total_orders,
    COALESCE(total_revenue, CAST(0 AS DECIMAL(14, 2))) AS total_revenue
FROM gold.daily_weekly_trends
WHERE period_type = 'day'
ORDER BY trend_date ASC;


-- =============================================================================
-- QUERY 2b (optional): Weekly Revenue Trend
-- Visualization : Line chart — weekly aggregation
-- Gold table(s) : gold.daily_weekly_trends
-- =============================================================================

-- SELECT
--     period_start AS trend_week_start,
--     COALESCE(total_orders, 0) AS total_orders,
--     COALESCE(total_revenue, CAST(0 AS DECIMAL(14, 2))) AS total_revenue
-- FROM gold.daily_weekly_trends
-- WHERE period_type = 'week'
-- ORDER BY trend_week_start ASC;


-- =============================================================================
-- QUERY 3: Customer Segmentation Mix (Behavioral)
-- Visualization : Pie chart or Donut chart
-- Question      : What share of customers fall into each behavioral segment?
-- Gold table(s) : gold.customer_segmentation
-- Notes         : Segments: High-Value, Repeat, One-Time, Inactive (mutually exclusive).
-- =============================================================================

SELECT
    segment_type,
    COALESCE(customer_count, 0) AS customer_count,
    COALESCE(total_revenue, CAST(0 AS DECIMAL(14, 2))) AS total_revenue,
    COALESCE(avg_revenue, CAST(0 AS DECIMAL(14, 2))) AS avg_revenue
FROM gold.customer_segmentation
WHERE COALESCE(customer_count, 0) > 0
ORDER BY total_revenue DESC;


-- =============================================================================
-- QUERY 4: Revenue by Customer Segment (Premium / Standard / Basic)
-- Visualization : Bar chart — segment performance comparison
-- Question      : Which customer tier (Premium/Standard/Basic) drives most revenue?
-- Gold table(s) : gold.revenue_by_customer
-- Notes         : Aggregates from customer grain; COUNT DISTINCT not needed (one row/customer).
-- =============================================================================

SELECT
    COALESCE(customer_segment, 'Unknown') AS customer_segment,
    COUNT(customer_id) AS customer_count,
    CAST(COALESCE(SUM(total_revenue), 0) AS DECIMAL(14, 2)) AS total_revenue,
    CAST(
        CASE
            WHEN SUM(COALESCE(total_orders, 0)) = 0 THEN NULL
            ELSE SUM(COALESCE(total_revenue, 0)) / SUM(COALESCE(total_orders, 0))
        END AS DECIMAL(12, 2)
    ) AS avg_order_value
FROM gold.revenue_by_customer
GROUP BY COALESCE(customer_segment, 'Unknown')
ORDER BY total_revenue DESC;


-- =============================================================================
-- QUERY 5: KPI Summary
-- Visualization : KPI cards / single-value indicators
-- Question      : What are total revenue, orders, AOV, customers, and top product?
-- Gold table(s) : gold.revenue_by_customer, gold.daily_weekly_trends, gold.sales_by_product
-- Notes         : Revenue/orders sourced from daily trends (day grain) to match
--                 enterprise-wide totals without summing customer-level orders twice.
--                 Top product from sales_by_product subquery.
-- =============================================================================

WITH daily_totals AS (
    SELECT
        CAST(COALESCE(SUM(total_revenue), 0) AS DECIMAL(14, 2)) AS total_revenue,
        CAST(COALESCE(SUM(total_orders), 0) AS BIGINT) AS total_orders
    FROM gold.daily_weekly_trends
    WHERE period_type = 'day'
),
customer_totals AS (
    SELECT
        CAST(COUNT(customer_id) AS BIGINT) AS total_customers
    FROM gold.revenue_by_customer
),
top_product AS (
    SELECT
        product_name AS top_product_by_revenue
    FROM gold.sales_by_product
    WHERE COALESCE(total_revenue, 0) > 0
    ORDER BY total_revenue DESC, product_name ASC
    LIMIT 1
)
SELECT
    d.total_revenue,
    d.total_orders,
    CAST(
        CASE
            WHEN d.total_orders = 0 THEN NULL
            ELSE d.total_revenue / d.total_orders
        END AS DECIMAL(12, 2)
    ) AS average_order_value,
    c.total_customers,
    t.top_product_by_revenue
FROM daily_totals AS d
CROSS JOIN customer_totals AS c
CROSS JOIN top_product AS t;


-- =============================================================================
-- VALIDATION NOTES (schema alignment)
-- -----------------------------------------------------------------------------
-- gold.sales_by_product
--   columns used: product_name, category, total_orders, total_revenue
--
-- gold.revenue_by_customer
--   columns used: customer_id, customer_segment, total_orders, total_revenue
--
-- gold.daily_weekly_trends
--   columns used: period_type, period_start, total_orders, total_revenue
--
-- gold.customer_segmentation
--   columns used: segment_type, customer_count, total_revenue, avg_revenue
--
-- Assumptions:
--   1. Gold tables already exclude invalid/quarantined Silver rows.
--   2. Gold revenue reflects Completed orders only (built in Gold layer).
--   3. Query 4 uses source customer_segment (Premium/Standard/Basic), not
--      behavioral segment_type from customer_segmentation — different dimensions.
--   4. KPI total_orders/revenue uses daily_weekly_trends (day) for global totals;
--      customer_count uses revenue_by_customer row count (all valid customers).
--   5. Products with zero revenue are excluded from Query 1 Top-N chart only.
-- =============================================================================
