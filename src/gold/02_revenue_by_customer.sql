-- Gold: revenue_by_customer
-- Source: valid Silver customers LEFT JOIN qualifying orders
-- Grain: one row per customer_id

CREATE OR REPLACE TABLE {gold_db}.revenue_by_customer
USING DELTA
AS
WITH qualifying_orders AS (
    SELECT
        order_id,
        customer_id,
        CAST(total_amount AS DECIMAL(12, 2)) AS total_amount
    FROM {silver_db}.silver_orders
    WHERE order_status = '{qualifying_status}'
),
customer_revenue AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        COUNT(DISTINCT o.order_id) AS total_orders,
        CAST(COALESCE(SUM(o.total_amount), 0) AS DECIMAL(14, 2)) AS total_revenue
    FROM {silver_db}.silver_customers AS c
    LEFT JOIN qualifying_orders AS o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name, c.customer_segment
)
SELECT
    customer_id,
    customer_name,
    customer_segment,
    total_orders,
    total_revenue,
    CAST(
        CASE
            WHEN total_orders = 0 THEN NULL
            ELSE total_revenue / total_orders
        END AS DECIMAL(12, 2)
    ) AS avg_order_value,
    total_revenue AS lifetime_value_actual
FROM customer_revenue;
