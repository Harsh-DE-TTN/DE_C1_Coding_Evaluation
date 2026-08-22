# Data Quality Strategy

## Quality Checks Overview

### 1. Completeness Check

- **What:** No NULLs in critical fields
- **How:** COUNT NULL values in email, customer_id, product_id
- **Threshold:** >99% complete
- **Result:** Flag rows with NULLs

### 2. Uniqueness Check

- **What:** No duplicate rows
- **How:** Check for duplicate order_id, customer_id
- **Threshold:** 100% unique
- **Result:** Flag duplicate rows

### 3. Referential Integrity

- **What:** Foreign keys exist in parent tables
- **How:** Check customer_id in customers, product_id in products
- **Threshold:** >99.9% valid
- **Result:** Flag orphan records



## Quality Metrics Report

[How you'll present % passed per check]

## Sample Data Quality Issues

[List the ~700 intentional issues in your sample data]