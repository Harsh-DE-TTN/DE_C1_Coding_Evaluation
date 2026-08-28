-- Gold: customer_segmentation
-- Source: gold.revenue_by_customer (built from valid Silver data)
-- Segments (mutually exclusive, priority order documented in data-quality-strategy.md):
--   Inactive  -> 0 completed orders
--   One-Time  -> exactly 1 completed order
--   High-Value -> 2+ completed orders AND total_revenue >= threshold
--   Repeat    -> 2+ completed orders AND total_revenue < threshold

CREATE OR REPLACE TABLE {gold_db}.customer_segmentation
USING DELTA
AS
WITH segment_spine AS (
    SELECT 'High-Value' AS segment_type
    UNION ALL SELECT 'Repeat'
    UNION ALL SELECT 'One-Time'
    UNION ALL SELECT 'Inactive'
),
classified AS (
    SELECT
        customer_id,
        total_revenue,
        total_orders,
        CASE
            WHEN total_orders = 0 THEN 'Inactive'
            WHEN total_orders = 1 THEN 'One-Time'
            WHEN total_orders >= 2 AND total_revenue >= CAST({high_value_threshold} AS DECIMAL(14, 2)) THEN 'High-Value'
            WHEN total_orders >= 2 THEN 'Repeat'
        END AS segment_type
    FROM {gold_db}.revenue_by_customer
),
aggregated AS (
    SELECT
        segment_type,
        COUNT(customer_id) AS customer_count,
        CAST(COALESCE(AVG(total_revenue), 0) AS DECIMAL(14, 2)) AS avg_revenue,
        CAST(COALESCE(SUM(total_revenue), 0) AS DECIMAL(14, 2)) AS total_revenue
    FROM classified
    GROUP BY segment_type
)
SELECT
    s.segment_type,
    COALESCE(a.customer_count, 0) AS customer_count,
    COALESCE(a.avg_revenue, CAST(0 AS DECIMAL(14, 2))) AS avg_revenue,
    COALESCE(a.total_revenue, CAST(0 AS DECIMAL(14, 2))) AS total_revenue
FROM segment_spine AS s
LEFT JOIN aggregated AS a
    ON s.segment_type = a.segment_type
ORDER BY
    CASE s.segment_type
        WHEN 'High-Value' THEN 1
        WHEN 'Repeat' THEN 2
        WHEN 'One-Time' THEN 3
        WHEN 'Inactive' THEN 4
    END;
