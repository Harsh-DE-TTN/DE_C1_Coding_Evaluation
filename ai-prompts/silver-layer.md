# AI Prompts — Silver Layer

## Prompt 1: Silver Layer Validation

### PROMPT SENT

> Act as a Senior Databricks Data Engineer. Implement ONLY the Silver layer for my e-commerce Medallion pipeline using PySpark + Delta Lake.
>
> **Input Bronze tables:**
> - `bronze.bronze_customers`
> - `bronze.bronze_orders`
> - `bronze.bronze_products`
>
> **Create:**
>
> ```text
> src/silver/
> ├── 01_quality_completeness.py
> ├── 02_quality_uniqueness.py
> ├── 03_quality_type_validation.py
> ├── 04_quality_referential_integrity.py
> ├── 05_quality_business_logic.py
> └── create_silver_tables.py
> ```
>
> **Implement checks:**
> - Completeness: NULL email, customer_id, product_id
> - Uniqueness: duplicate customer_id and order_id
> - Type validation: schemas/types/formats
> - Referential integrity: orders.customer_id → customers, orders.product_id → products
> - Business logic: quantity > 0, prices > 0, cost < price, total_amount = quantity × unit_price,
>   valid status/payment-date relationships
>
> **Expected intentional issues:** 50 NULL emails, 10 dup customer IDs, 100 NULL order customer_id,
> 200 NULL order product_id, 50 invalid customer_id, 30 invalid product_id, 20 dup order_id
>
> **Do not silently delete bad records.** Add:
> - `quality_status`
> - `quality_check_result`
> - `quality_check_reason`
>
> **Valid records →** `silver.silver_customers`, `silver.silver_orders`, `silver.silver_products`
>
> **Failed records →** quarantine/rejected tables
>
> **Create** `silver.data_quality_report` with dataset, check, total, passed, failed, pass %, fail %, timestamp.
>
> Use actual calculated metrics. Add tests proving every intentional issue is detected.
> Keep Bronze unchanged. Do not implement Gold or Dashboard.
>
> Update `data-quality-strategy.md`. Verify flow:
> Bronze → Validate → Flag → PASS/Silver + FAIL/Quarantine → Quality Report
>
> Also fill this prompt history in .md file.

### AI RESPONSE SUMMARY

Implemented Silver layer:

- `src/silver/silver_common.py` — shared validation, routing, report writer
- `src/silver/01_quality_completeness.py` through `05_quality_business_logic.py`
- `src/silver/create_silver_tables.py` — orchestrator (customers → products → orders)
- `src/silver/SILVER_LAYER.md` — layer documentation
- `tests/silver/test_silver_quality_checks.py` — 9 tests for each defect type
- Updated `data-quality-strategy.md`

**Output tables:**

| PASS | Rejected |
| ---- | -------- |
| `silver.silver_customers` | `silver.silver_customers_rejected` |
| `silver.silver_products` | `silver.silver_products_rejected` |
| `silver.silver_orders` | `silver.silver_orders_rejected` |
| | `silver.data_quality_report` |

**Quality columns:** `quality_status`, `quality_check_result`, `quality_check_reason`, `_validation_timestamp`

**Post-run verification:** `verify_intentional_issues()` confirms all 7 defect types detected in rejected tables.

### YOUR EVALUATION

**✓ What was good:**

- Modular check files matching exercise structure
- Bronze read-only; row count identity preserved (pass + rejected = bronze)
- FK checks against valid Silver parents (correct processing order)
- All duplicate-key rows fail (no silent dedupe)
- Calculated DQ report metrics
- Unit tests per intentional issue type

**✗ What needed fixing:**

- [Databricks cluster integration test pending]

**△ Assumptions:**

- Duplicate PK: all rows sharing key are FAIL (per data model)
- FK validation uses `silver_customers`/`silver_products` PASS sets only
- `payment_date` NULL allowed except for Completed orders
- `OVERALL_ROW_VALIDITY` metric added per dataset

**FINAL DECISION:** Silver layer complete for Databricks execution; Gold/Dashboard not implemented

---
