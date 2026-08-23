# Requirement Analysis
## Problem Statement
E-commerce company receives customer, order and product
data from multiple sources.

The company wants a Databricks pipeline that:
1. Ingests raw data
2. Validates and cleans data
3. Creates business-ready aggregations
4. Provides dashboard insights

## Functional Requirements
1. Generate customers.csv
2. Generate orders.csv
3. Generate products.csv
4. Introduce intentional data quality issues
5. Ingest data into Bronze
6. Validate data in Silver
7. Generate quality metrics
8. Create Gold aggregations
9. Create dashboard queries
10. Create tests

## Non-Functional Requirements
1. Code should be maintainable
2. Pipeline should be reproducible
3. Data quality failures should be visible
4. Bronze should preserve raw data
5. No secrets should be hardcoded
6. Pipeline should have error handling## Assumptions

## Edge Cases
1. Customer data may contain duplicate customer_id values.
2. Customer records may contain missing or NULL names, emails, cities, or other required fields.
3. Customer email addresses may have invalid formats.
4. Orders may contain duplicate order_id values.
5. Orders may reference a customer_id that does not exist in the customer data.
6. Orders may reference a product_id that does not exist in the product data.
7. Orders may contain NULL or invalid customer_id/product_id values.
8. Order amounts may be zero or negative.
9. Product prices may be zero, negative, or NULL.
10. Product records may contain duplicate product_id values.
11. CSV files may contain missing or unexpected columns.
12. CSV files may be empty or contain only headers.
13. Input data may contain unexpected data types, such as text in numeric columns.
14. Date fields may be NULL, invalid, or incorrectly formatted.
15. The same input file may be processed more than once, so the pipeline should avoid creating unintended duplicate records.
16. Some records may fail validation while other records are valid. Valid records should continue through the pipeline while invalid records should be captured and reported.
17. Input files may be missing or unavailable when the pipeline starts.
18. The pipeline may encounter unexpected errors during ingestion or transformation.
19. Data quality issues should not be silently discarded; they should be measurable and traceable.
20. Aggregations should not include invalid or rejected records unless explicitly required by the business.

## Clarifications Needed
1. What is the expected number of records for customers.csv, orders.csv, and products.csv?
2. What columns and data types are required in each CSV file?
3. Which customer fields are mandatory?
4. Which product fields are mandatory?
5. Which order fields are mandatory?
6. What should happen when an order references a customer_id that does not exist?
7. What should happen when an order references a product_id that does not exist?
8. Should duplicate customers, products, or orders be rejected, removed, or deduplicated?
9. What rules should define a valid email address?
10. Are zero or negative order amounts allowed?
11. Are zero or negative product prices allowed?
12. What should happen to records that fail data-quality validation: quarantine them, reject them, or fix them automatically?
13. What data-quality threshold should cause the pipeline to fail?
14. Which quality metrics are required in the final output?
15. Which business aggregations are required in the Gold layer?
16. Which dashboard KPIs are required?
17. How frequently should the pipeline run?
18. Should the pipeline support incremental processing or process the complete dataset each time?
19. What should happen if an input file is missing?
20. What logging and error-reporting mechanism should be used?
21. Which Databricks compute/environment should be used?
22. Where should secrets and configuration values be stored?
23. What testing framework and minimum test coverage are expected?
24. What is the expected behavior when a pipeline run partially fails?