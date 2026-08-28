# Data Quality Strategy

Silver-layer data quality for the e-commerce Medallion pipeline.

**Flow:** Bronze (raw) → Validate → Flag → PASS/`silver_*` + FAIL/`silver_*_rejected` → `silver.data_quality_report`

Bronze tables are **never modified**. Failed rows are **never silently deleted**.

---

## 1. Quality Check Catalog

### Completeness (`01_quality_completeness.py`)

| Dataset | Check code | Rule | Threshold |
| ------- | ---------- | ---- | --------- |
| customers | `CUSTOMER_ID_NULL` | `customer_id` NOT NULL | 100% |
| customers | `CUSTOMER_EMAIL_NULL` | `email` NOT NULL | >99% (50 known NULLs) |
| orders | `ORDER_CUSTOMER_ID_NULL` | `customer_id` NOT NULL | >99% (100 known NULLs) |
| orders | `ORDER_PRODUCT_ID_NULL` | `product_id` NOT NULL | >99% (200 known NULLs) |
| products | `PRODUCT_ID_NULL` | `product_id` NOT NULL | 100% |

### Uniqueness (`02_quality_uniqueness.py`)

| Dataset | Check code | Rule | Threshold |
| ------- | ---------- | ---- | --------- |
| customers | `DUPLICATE_CUSTOMER_ID` | `customer_id` unique | 100% in PASS table |
| orders | `DUPLICATE_ORDER_ID` | `order_id` unique | 100% in PASS table |
| products | `DUPLICATE_PRODUCT_ID` | `product_id` unique | 100% |

Duplicate keys: **all rows sharing the key are FAIL** (no survivor selection).

### Type / Format (`03_quality_type_validation.py`)

| Dataset | Checks | Rule |
| ------- | ------ | ---- |
| customers | name, segment, signup_date, lifetime_value | Required fields; segment ∈ Premium/Standard/Basic |
| orders | dates, quantities, amounts, status | Required typed fields; status ∈ Pending/Completed/Cancelled |
| products | name, price, cost, stock, reorder | Required typed fields |

### Referential Integrity (`04_quality_referential_integrity.py`)

| Dataset | Check code | Rule | Threshold |
| ------- | ---------- | ---- | --------- |
| orders | `FK_CUSTOMER_MISSING` | `customer_id` exists in **valid** `silver_customers` | >99.9% |
| orders | `FK_PRODUCT_MISSING` | `product_id` exists in **valid** `silver_products` | >99.9% |

FK validation runs **after** customers and products are validated. Orphan orders → rejected.

### Business Logic (`05_quality_business_logic.py`)

| Dataset | Check code | Rule |
| ------- | ---------- | ---- |
| products | `PRODUCT_PRICE_NOT_POSITIVE` | `price > 0` |
| products | `PRODUCT_COST_NOT_POSITIVE` | `cost > 0` |
| products | `PRODUCT_COST_NOT_LESS_THAN_PRICE` | `cost < price` |
| products | `PRODUCT_STOCK_NEGATIVE` | `stock_quantity >= 0` |
| products | `PRODUCT_REORDER_NEGATIVE` | `reorder_level >= 0` |
| orders | `ORDER_QUANTITY_NOT_POSITIVE` | `quantity > 0` |
| orders | `ORDER_UNIT_PRICE_NOT_POSITIVE` | `unit_price > 0` |
| orders | `ORDER_TOTAL_AMOUNT_NOT_POSITIVE` | `total_amount > 0` |
| orders | `ORDER_AMOUNT_MISMATCH` | `total_amount ≈ quantity × unit_price` (±0.01) |
| orders | `PAYMENT_DATE_MISSING_FOR_COMPLETED` | Completed → `payment_date` required |
| orders | `PAYMENT_DATE_UNEXPECTED_FOR_NON_COMPLETED` | Pending/Cancelled → `payment_date` NULL |

---

## 2. Row-Level Quality Columns

Every evaluated row receives:

| Column | PASS value | FAIL value |
| ------ | ---------- | ---------- |
| `quality_status` | `PASS` | `FAIL` |
| `quality_check_result` | `PASSED` | `FAILED` |
| `quality_check_reason` | `NULL` | `;`-separated failed check codes |
| `_validation_timestamp` | run timestamp | run timestamp |

---

## 3. Output Tables

| Table | Contents |
| ----- | -------- |
| `silver.silver_customers` | Valid customers only |
| `silver.silver_customers_rejected` | Failed customers + quality columns |
| `silver.silver_products` | Valid products only |
| `silver.silver_products_rejected` | Failed products |
| `silver.silver_orders` | Valid orders only |
| `silver.silver_orders_rejected` | Failed orders |
| `silver.data_quality_report` | Per-check metrics (calculated, not hard-coded) |

### `silver.data_quality_report` schema

| Column | Description |
| ------ | ----------- |
| `dataset` | customers / orders / products |
| `check` | Check code (e.g. `CUSTOMER_EMAIL_NULL`) |
| `total` | Rows evaluated |
| `passed` | Rows passing this check |
| `failed` | Rows failing this check |
| `pass_pct` | `passed / total × 100` |
| `fail_pct` | `failed / total × 100` |
| `validation_timestamp` | Silver run timestamp |

Also includes `OVERALL_ROW_VALIDITY` per dataset.

---

## 4. Quarantine Strategy

1. Read Bronze unchanged.
2. Apply all checks; accumulate failures in `_failed_checks` array.
3. Set `quality_status` = PASS or FAIL.
4. **PASS** → `silver_<entity>`
5. **FAIL** → `silver_<entity>_rejected`
6. Row count identity: `bronze_count = pass_count + rejected_count` per entity.

No imputation, no FK fabrication, no dedupe-winner selection.

---

## 5. Failure Handling

| Scenario | Action |
| -------- | ------ |
| Row fails any check | Routed to `_rejected` with reason codes |
| Multiple failures on one row | All codes in `quality_check_reason` |
| Bronze table missing | Pipeline fails with clear error |
| Post-run defect verification fails | Pipeline fails (intentional issues not detected) |
| Partial Silver write | Investigate and re-run `create_silver_tables.py` |

Pipeline does **not** silently report success when verification fails.

---

## 6. Intentional Sample Data Issues (Expected Detection)

| Issue | Expected minimum in rejected | Silver check |
| ----- | ---------------------------: | ------------ |
| NULL customer email | 50 | `CUSTOMER_EMAIL_NULL` |
| Duplicate customer_id | 10 keys | `DUPLICATE_CUSTOMER_ID` |
| NULL order customer_id | 100 | `ORDER_CUSTOMER_ID_NULL` |
| NULL order product_id | 200 | `ORDER_PRODUCT_ID_NULL` |
| Invalid customer_id (orphan) | 50 | `FK_CUSTOMER_MISSING` |
| Invalid product_id (orphan) | 30 | `FK_PRODUCT_MISSING` |
| Duplicate order_id | 20 keys | `DUPLICATE_ORDER_ID` |

**Note:** 460 explicit injections; ~700 narrative count includes overlapping accounting. Distinct rejected rows may be fewer than sum of per-check failures.

---

## 7. Validation Results

Verified by:

- `create_silver_tables.verify_intentional_issues()` after each run
- `tests/silver/test_silver_quality_checks.py` — unit tests per defect type

Run tests:

```bash
pytest tests/silver/test_silver_quality_checks.py -v
```

Run Silver pipeline (Databricks):

```bash
python src/silver/create_silver_tables.py
```

---

## 8. Processing Order

```text
bronze.bronze_customers  ──► validate ──► silver_customers + silver_customers_rejected
bronze.bronze_products   ──► validate ──► silver_products  + silver_products_rejected
bronze.bronze_orders     ──► validate (FK vs valid parents) ──► silver_orders + silver_orders_rejected
                                                      └──► silver.data_quality_report
```

Gold and Dashboard read **only** `silver.silver_*` PASS tables (not yet implemented).
