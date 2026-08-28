-- =============================================================================
-- E-commerce Medallion Pipeline — Database / Schema Setup
-- =============================================================================
-- Run in Databricks SQL or a notebook to provision catalogs/schemas.
-- Tables are created by pipeline scripts (Delta format); this file defines
-- databases and documents expected table names.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Create schemas (databases)
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Raw landing layer — immutable source CSV payloads + ingest metadata';

CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Validated layer — PASS tables, rejected quarantine, DQ report';

CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Business-ready aggregations for analytics and dashboards';

-- -----------------------------------------------------------------------------
-- 2. Bronze tables (created by src/bronze/ingest_all.py)
-- -----------------------------------------------------------------------------

-- bronze.bronze_customers
--   customer_id INT, customer_name STRING, email STRING, country STRING,
--   signup_date DATE, customer_segment STRING, lifetime_value DECIMAL(12,2),
--   _ingestion_timestamp TIMESTAMP, _ingestion_date DATE, _source_file STRING

-- bronze.bronze_orders
--   order_id INT, customer_id INT, order_date DATE, product_id INT,
--   quantity INT, unit_price DECIMAL(10,2), total_amount DECIMAL(12,2),
--   order_status STRING, payment_date DATE,
--   _ingestion_timestamp TIMESTAMP, _ingestion_date DATE, _source_file STRING

-- bronze.bronze_products
--   product_id INT, product_name STRING, category STRING,
--   price DECIMAL(10,2), cost DECIMAL(10,2),
--   stock_quantity INT, reorder_level INT,
--   _ingestion_timestamp TIMESTAMP, _ingestion_date DATE, _source_file STRING

-- -----------------------------------------------------------------------------
-- 3. Silver tables (created by src/silver/create_silver_tables.py)
-- -----------------------------------------------------------------------------

-- silver.silver_customers          — PASS rows
-- silver.silver_customers_rejected — FAIL rows + quality columns
-- silver.silver_orders
-- silver.silver_orders_rejected
-- silver.silver_products
-- silver.silver_products_rejected
-- silver.data_quality_report
--   dataset, check, total, passed, failed, pass_pct, fail_pct, validation_timestamp

-- Silver quality columns on all evaluated rows:
--   quality_status, quality_check_result, quality_check_reason, _validation_timestamp

-- -----------------------------------------------------------------------------
-- 4. Gold tables (created by src/gold/create_gold_tables.py)
-- -----------------------------------------------------------------------------

-- gold.sales_by_product
--   product_id, product_name, category, total_orders, total_revenue, avg_order_value

-- gold.revenue_by_customer
--   customer_id, customer_name, customer_segment, total_orders, total_revenue,
--   avg_order_value, lifetime_value_actual

-- gold.customer_segmentation
--   segment_type (High-Value|Repeat|One-Time|Inactive),
--   customer_count, avg_revenue, total_revenue

-- -----------------------------------------------------------------------------
-- 5. Verification queries (run after pipeline)
-- -----------------------------------------------------------------------------

-- SELECT COUNT(*) FROM bronze.bronze_customers;
-- SELECT COUNT(*) FROM silver.silver_customers;
-- SELECT COUNT(*) FROM silver.silver_customers_rejected;
-- SELECT * FROM silver.data_quality_report ORDER BY dataset, check;
-- SELECT COUNT(*) FROM gold.sales_by_product;
