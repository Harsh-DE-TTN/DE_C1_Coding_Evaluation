# Data Generation Notes

Documentation for `generate_sample_data.py` — the sample e-commerce CSV generator for the Medallion Architecture pipeline exercise.

---

## 1. Purpose

Produce **realistic, reproducible** e-commerce source files (`customers.csv`, `products.csv`, `orders.csv`) with **intentional data-quality defects** so the later **Silver** layer can exercise:

- Completeness checks
- Uniqueness checks
- Referential integrity checks
- Type / business-rule validation

Bronze, Silver, Gold, and Dashboard are **out of scope** for this component.

---

## 2. Dataset Sizes and Actual File Sizes


| Dataset   | Target rows | Actual rows | Target size | Actual size (KB) |
| --------- | ----------- | ----------- | ----------- | ---------------- |
| customers | 10,000      | 10,000      | ~500 KB     | 775.8            |
| products  | 500         | 500         | ~50 KB      | 23.8             |
| orders    | 100,000     | 100,000     | ~2–3 MB     | 6,320.0          |


**Note on file sizes:** Actual sizes exceed the approximate targets because Faker produces long names, emails, and country strings. Row counts match the specification exactly.

**Output path:** `data/` (repo root)

---



## 3. Complete Schema



### customers.csv


| Column           | Type    | Notes                                     |
| ---------------- | ------- | ----------------------------------------- |
| customer_id      | INT     | Primary key (intended unique)             |
| customer_name    | STRING  | Faker-generated full name                 |
| email            | STRING  | Faker email; 50 intentional NULLs         |
| country          | STRING  | Faker country name                        |
| signup_date      | DATE    | `YYYY-MM-DD`, 2020-01-01 through today    |
| customer_segment | STRING  | Premium / Standard / Basic                |
| lifetime_value   | DECIMAL | Two decimal places; segment-driven ranges |




### products.csv


| Column         | Type    | Notes                           |
| -------------- | ------- | ------------------------------- |
| product_id     | INT     | Primary key                     |
| product_name   | STRING  | Two-word product name           |
| category       | STRING  | One of 10 e-commerce categories |
| price          | DECIMAL | List price; always > 0          |
| cost           | DECIMAL | Always > 0 and < price          |
| stock_quantity | INT     | >= 0                            |
| reorder_level  | INT     | >= 0                            |




### orders.csv


| Column       | Type    | Notes                                          |
| ------------ | ------- | ---------------------------------------------- |
| order_id     | INT     | Primary key (intended unique)                  |
| customer_id  | INT     | FK → customers; intentional NULLs and orphans  |
| order_date   | DATE    | On or after customer signup, not in the future |
| product_id   | INT     | FK → products; intentional NULLs and orphans   |
| quantity     | INT     | > 0 on clean rows                              |
| unit_price   | DECIMAL | > 0 on clean rows                              |
| total_amount | DECIMAL | = quantity × unit_price on clean rows          |
| order_status | STRING  | Pending / Completed / Cancelled                |
| payment_date | DATE    | Nullable; set for Completed orders             |


---



## 4. Generation Approach and Libraries


| Library   | Role                                           |
| --------- | ---------------------------------------------- |
| `faker`   | Names, emails, countries, dates, product words |
| `pandas`  | DataFrames, CSV I/O, validation                |
| `random`  | Seeded distributions and issue-index selection |
| `decimal` | Precise monetary rounding                      |
| `pathlib` | Repo-relative output paths                     |




### Pipeline steps

1. `generate_customers()` — base customer dimension
2. `inject_customer_quality_issues()` — NULL emails, duplicate IDs
3. `generate_products()` — product catalog
4. `generate_orders()` — Pareto-weighted order assignment across customers
5. `inject_order_quality_issues()` — NULL FKs, invalid FKs, duplicate order IDs
6. Write CSVs to `data/`
7. `validate_generated_data()` — computed checks
8. `print_generation_summary()` — stdout report



### Realism techniques

- **Customer segments:** weighted 15% Premium, 35% Standard, 50% Basic
- **Lifetime value:** higher ranges for Premium customers
- **Order distribution:** Pareto-weighted counts so some customers have many orders, some one, and **10 customers have zero orders**
- **Product popularity:** weighted random product selection (heavy tail)
- **Order status:** ~82% Completed, ~10% Pending, ~8% Cancelled
- **Payment dates:** all Completed orders have `payment_date`; Pending/Cancelled have NULL
- **Unit price:** usually catalog price; ~8% promotional variance on clean rows

---



## 5. Random Seed and Reproducibility

```python
CUSTOMER_COUNT = 10_000
ORDER_COUNT = 100_000
PRODUCT_COUNT = 500
RANDOM_SEED = 42
```

Both `random.Random(42)` and `Faker.seed(42)` are set in `_setup_random()`. Re-running the script with the same seed and configuration produces identical output.

Issue row indices are chosen once via `build_issue_plan()` using disjoint index pools per issue type (no overlap between order issue categories).

---



## 6. Data Distributions and Relationships



### Customer segments (actual)


| Segment  | Count |
| -------- | ----- |
| Basic    | 4,976 |
| Standard | 3,512 |
| Premium  | 1,512 |




### Order status (actual)


| Status    | Count  |
| --------- | ------ |
| Completed | 81,894 |
| Pending   | 10,060 |
| Cancelled | 8,046  |




### Product categories

Evenly spread across 10 categories (~50–61 products each).

### Customer–order coverage


| Metric                         | Count |
| ------------------------------ | ----- |
| Customers with ≥ 1 valid order | 9,990 |
| Customers with zero orders     | 10    |


Valid FK references use `customer_id` 1–10,000 and `product_id` 1–500.

---



## 7. Every Intentional Quality Issue


| Dataset   | Issue                 | Expected | Actual | Injection method                                      |
| --------- | --------------------- | -------- | ------ | ----------------------------------------------------- |
| customers | NULL email            | 50       | 50     | Set `email` to empty on 50 pre-selected rows          |
| customers | Duplicate customer_id | 10       | 10     | Copy `customer_id` from 10 target rows onto 10 others |
| orders    | NULL customer_id      | 100      | 100    | Set `customer_id` to empty on 100 rows                |
| orders    | NULL product_id       | 200      | 200    | Set `product_id` to empty on 200 rows                 |
| orders    | Invalid customer_id   | 50       | 50     | Assign IDs 90001–90050 (outside valid range)          |
| orders    | Invalid product_id    | 30       | 30     | Assign IDs 9001–9030 (outside valid range)            |
| orders    | Duplicate order_id    | 20       | 20     | Copy `order_id` from 20 source rows onto 20 others    |


Invalid foreign keys **never** use IDs in the valid customer (1–10,000) or product (1–500) ranges.

---



## 8. Expected vs Actual Issue Counts



### Listed injections (assignment explicit list)

```text
50 + 10 + 100 + 200 + 50 + 30 + 20 = 460 issue injections
```


| Check                        | Expected | Actual  |
| ---------------------------- | -------- | ------- |
| NULL customer emails         | 50       | 50      |
| Duplicate customer_id rows   | 10       | 10      |
| NULL order customer_id       | 100      | 100     |
| NULL order product_id        | 200      | 200     |
| Invalid order customer_id    | 50       | 50      |
| Invalid order product_id     | 30       | 30      |
| Duplicate order_id rows      | 20       | 20      |
| **Injection total (listed)** | **460**  | **460** |




### ~700 problematic rows (assignment narrative)

The assignment mentions approximately **700 problematic rows**, but only **460 explicit injections** are defined. This generator implements **only the explicit list** and does **not** invent extra defects to reach 700.

**Distinct order rows with at least one injected issue:** 400 (disjoint index pools per issue type).

**Overlap accounting:**

- Customer issues affect **60 distinct rows** (50 NULL email + 10 duplicate ID; disjoint sets).
- Order issues affect **400 distinct rows** (100 + 200 + 50 + 30 + 20; disjoint sets).
- No order row carries more than one injected issue type.
- Total distinct problematic rows across all files: **460** (not 700).

---



## 9. Why Each Issue Exists and Silver Detection


| Issue                 | Why it exists                        | Silver check                          |
| --------------------- | ------------------------------------ | ------------------------------------- |
| NULL email            | Test completeness on customer email  | `01_quality_completeness.py`          |
| Duplicate customer_id | Test uniqueness on customer PK       | `02_quality_uniqueness.py`            |
| NULL customer_id      | Test completeness on order FK        | `01_quality_completeness.py`          |
| NULL product_id       | Test completeness on order FK        | `01_quality_completeness.py`          |
| Invalid customer_id   | Test referential integrity (orphans) | `04_quality_referential_integrity.py` |
| Invalid product_id    | Test referential integrity (orphans) | `04_quality_referential_integrity.py` |
| Duplicate order_id    | Test uniqueness on order PK          | `02_quality_uniqueness.py`            |


Type and business-rule checks (`03_quality_type_validation.py`, `05_quality_business_logic.py`) apply to the remaining clean rows.

---



## 10. Validation Results

Built-in validation (`validate_generated_data()`) runs after every generation. Latest run:

```text
Validation: PASSED
Random seed: 42
```

Checks performed:

- [x] All three CSV files exist
- [x] Required columns present
- [x] Row counts: 10,000 / 500 / 100,000
- [x] NULL email count = 50
- [x] Duplicate customer_id rows = 10
- [x] NULL order customer_id = 100
- [x] NULL order product_id = 200
- [x] Invalid customer_id count = 50
- [x] Invalid product_id count = 30
- [x] Duplicate order_id rows = 20
- [x] Clean orders: quantity > 0, unit_price > 0, total_amount ≈ quantity × unit_price
- [x] Products: price > 0, cost > 0, cost < price

Run locally:

```bash
python3 -m pip install faker pandas
python3 src/data_generation/generate_sample_data.py
```

---



## 11. Assumptions and Deviations



### Assumptions

1. **Invalid FK IDs** use ranges 90001+ (customers) and 9001+ (products) — guaranteed outside valid IDs.
2. **Duplicate keys:** 10 customer rows and 20 order rows are reassigned to duplicate an existing key (not extra rows appended).
3. **NULL representation** in CSV is an empty field (`na_rep=""`).
4. **Signup dates** span 2020-01-01 through generation date; no future signup dates on clean rows.
5. **Completed orders** always receive a `payment_date`; Pending/Cancelled leave it NULL.
6. **Issue index pools** are disjoint per issue type for clarity and predictable counts.



### Deviations from approximate size targets


| File      | Target  | Actual  | Reason                          |
| --------- | ------- | ------- | ------------------------------- |
| customers | ~500 KB | ~776 KB | Long Faker strings              |
| orders    | ~2–3 MB | ~6.3 MB | Full decimal precision + volume |


Row counts match exactly; only file sizes deviate from approximate targets.

### Not implemented (by design)

- No extra quality issues beyond the explicit 460 injections
- No Bronze / Silver / Gold / Dashboard code
- No future `signup_date` defects (not in the final explicit issue list for this exercise)

---



## AI-Assisted Engineering Log


| Step                  | Outcome                                                             |
| --------------------- | ------------------------------------------------------------------- |
| Requirements analysis | Implemented only explicit 460 injections; documented 700 vs 460 gap |
| Generation approach   | Seeded Faker + Pareto order distribution + disjoint issue pools     |
| Edge cases            | Invalid FK collision avoidance; clean-row business-rule validation  |
| Implementation        | `generate_sample_data.py` with modular functions                    |
| Validation            | All checks passed on seed 42                                        |
| Accepted              | Explicit issue counts, disjoint injections, reproducibility         |
| Changed               | Used invalid ID ranges 90001+/9001+ instead of ad-hoc negatives     |
| Rejected              | Adding ~240 extra defects to reach 700 without specification        |
| Assumptions           | Documented in Section 11                                            |


---



## Quick Reference — Latest Generation Summary

```text
Row counts:     customers 10,000 | products 500 | orders 100,000
File sizes KB:  customers 775.8  | products 23.8 | orders 6,320.0
NULL emails:    50
Dup customer:   10 rows
NULL cust FK:   100
NULL prod FK:   200
Invalid cust:   50
Invalid prod:   30
Dup order:      20 rows
Injection sum:  460
Distinct bad order rows: 400
Seed:           42
Path:           data/
Validation:     PASSED
```

