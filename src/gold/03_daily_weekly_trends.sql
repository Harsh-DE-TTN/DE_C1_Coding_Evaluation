-- Gold: daily_weekly_trends
-- Source: qualifying Silver orders only
-- Grain: period_type + period_start (day and week)

CREATE OR REPLACE TABLE {gold_db}.daily_weekly_trends
USING DELTA
AS
WITH qualifying_orders AS (
    SELECT
        order_id,
        order_date,
        CAST(total_amount AS DECIMAL(12, 2)) AS total_amount
    FROM {silver_db}.silver_orders
    WHERE order_status = '{qualifying_status}'
      AND order_date IS NOT NULL
),
daily AS (
    SELECT
        'day' AS period_type,
        order_date AS period_start,
        COUNT(DISTINCT order_id) AS total_orders,
        CAST(SUM(total_amount) AS DECIMAL(14, 2)) AS total_revenue
    FROM qualifying_orders
    GROUP BY order_date
),
weekly AS (
    SELECT
        'week' AS period_type,
        DATE_TRUNC('week', order_date) AS period_start,
        COUNT(DISTINCT order_id) AS total_orders,
        CAST(SUM(total_amount) AS DECIMAL(14, 2)) AS total_revenue
    FROM qualifying_orders
    GROUP BY DATE_TRUNC('week', order_date)
),
combined AS (
    SELECT * FROM daily
    UNION ALL
    SELECT * FROM weekly
)
SELECT
    period_type,
    CAST(period_start AS DATE) AS period_start,
    total_orders,
    total_revenue,
    CAST(
        CASE
            WHEN total_orders = 0 THEN NULL
            ELSE total_revenue / total_orders
        END AS DECIMAL(12, 2)
    ) AS avg_order_value
FROM combined;
