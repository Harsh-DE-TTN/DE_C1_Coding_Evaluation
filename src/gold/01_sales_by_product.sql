-- Gold: sales_by_product
-- Source: valid Silver orders (Completed only) + valid Silver products
-- Grain: one row per product_id

CREATE OR REPLACE TABLE {gold_db}.sales_by_product
USING DELTA
AS
WITH qualifying_orders AS (
    SELECT
        order_id,
        product_id,
        CAST(total_amount AS DECIMAL(12, 2)) AS total_amount
    FROM {silver_db}.silver_orders
    WHERE order_status = '{qualifying_status}'
),
product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        CAST(COALESCE(SUM(o.total_amount), 0) AS DECIMAL(14, 2)) AS total_revenue
    FROM {silver_db}.silver_products AS p
    LEFT JOIN qualifying_orders AS o
        ON p.product_id = o.product_id
    GROUP BY p.product_id, p.product_name, p.category
)
SELECT
    product_id,
    product_name,
    category,
    total_orders,
    total_revenue,
    CAST(
        CASE
            WHEN total_orders = 0 THEN NULL
            ELSE total_revenue / total_orders
        END AS DECIMAL(12, 2)
    ) AS avg_order_value
FROM product_sales;
