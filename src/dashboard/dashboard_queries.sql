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
-- | 2 | customer_segmentation_mix      | Pie / Donut chart    | How are customers split by behavior?       | customer_segmentation            |
-- | 3 | revenue_by_customer_segment  | Bar chart            | How does revenue vary by customer tier?    | revenue_by_customer              |
-- | 4 | kpi_summary                    | KPI cards            | What are overall business health metrics?  | sales_by_product, revenue_by_customer |
-- =============================================================================


-- =============================================================================
-- QUERY 1: Top Products by Revenue
-- Visualization : Bar chart (horizontal or vertical)
-- Question      : Which products generate the highest completed-order revenue?
-- Gold table(s) : gold.sales_by_product
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
-- QUERY 2: Customer Segmentation Mix (Behavioral)
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
-- QUERY 3: Revenue by Customer Segment (Premium / Standard / Basic)
-- Visualization : Bar chart — segment performance comparison
-- Question      : Which customer tier (Premium/Standard/Basic) drives most revenue?
-- Gold table(s) : gold.revenue_by_customer
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
-- QUERY 4: KPI Summary
-- Visualization : KPI cards / single-value indicators
-- Question      : What are total revenue, orders, AOV, customers, and top product?
-- Gold table(s) : gold.revenue_by_customer, gold.sales_by_product
-- =============================================================================

WITH customer_totals AS (
    SELECT
        CAST(COALESCE(SUM(total_revenue), 0) AS DECIMAL(14, 2)) AS total_revenue,
        CAST(COALESCE(SUM(total_orders), 0) AS BIGINT) AS total_orders,
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
    c.total_revenue,
    c.total_orders,
    CAST(
        CASE
            WHEN c.total_orders = 0 THEN NULL
            ELSE c.total_revenue / c.total_orders
        END AS DECIMAL(12, 2)
    ) AS average_order_value,
    c.total_customers,
    t.top_product_by_revenue
FROM customer_totals AS c
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
-- gold.customer_segmentation
--   columns used: segment_type, customer_count, total_revenue, avg_revenue
-- =============================================================================
