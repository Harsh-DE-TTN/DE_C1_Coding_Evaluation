# AI Prompts — Data Generation

## Prompt 1: Initial Data Generation Script

### PROMPT SENT

> "Generate Python script to create realistic e-commerce customer data.
> I need 10,000 rows with these fields: customer_id (INT), customer_name (STRING),
> email (STRING), country (STRING), signup_date (DATE between 2020-2024),
> customer_segment (Premium/Standard/Basic), lifetime_value (DECIMAL).
> Include realistic values like actual names, valid email formats, and random dates."

### AI RESPONSE SUMMARY

[Cursor generated Python script using faker library to create realistic data]

### YOUR EVALUATION

**✓ What was good:** Faker names/emails, date range, segment randomization

**✗ What needed fixing:** Future signup dates; no intentional quality issues; missing LTV logic

---

## Prompt 2: Adding Quality Issues (Iteration)

### PROMPT SENT

> "Modify the script to introduce intentional quality issues for testing:
> - 50 rows with NULL email
> - 10 rows with duplicate customer_id
> - 30 rows with signup_date > today()
> Keep the rest realistic. Add comments explaining the quality issues."

### AI RESPONSE SUMMARY

[Cursor modified script to add quality issues and comments]

### YOUR EVALUATION

**✓ ACCEPTED** — Intentional issues included and commented

---

## Prompt 3: Full Sample Data Generation (AI Capability Exercise)

### PROMPT SENT

> Act as a **Senior Data Engineer specializing in Python, PySpark, Databricks, e-commerce data, and data quality**.
>
> I am building the **AI Capability Exercise – E-commerce Medallion Architecture Pipeline**.
> For this task, implement **ONLY the Sample Data Generation component**.
> Do not implement Bronze, Silver, Gold, or Dashboard.
>
> Create:
>
> ```text
> data/customers.csv, data/orders.csv, data/products.csv
> src/data_generation/generate_sample_data.py
> src/data_generation/DATA_GENERATION_NOTES.md
> ```
>
> **customers.csv** — 10,000 rows: customer_id, customer_name, email, country,
> signup_date, customer_segment, lifetime_value. Issues: 50 NULL email, 10 duplicate customer_id.
>
> **products.csv** — 500 rows: product_id, product_name, category, price, cost,
> stock_quantity, reorder_level. Normal: price>0, cost>0, cost<price.
>
> **orders.csv** — 100,000 rows: order_id, customer_id, order_date, product_id,
> quantity, unit_price, total_amount, order_status, payment_date (nullable).
> Issues: 100 NULL customer_id, 200 NULL product_id, 50 invalid customer_id,
> 30 invalid product_id, 20 duplicate order_id (= 460 injections, not ~700).
>
> Use RANDOM_SEED=42, configurable counts, pathlib, validation, realistic distributions,
> post-generation summary with calculated metrics. Fill DATA_GENERATION_NOTES.md.

### AI RESPONSE SUMMARY

Implemented `generate_sample_data.py` with modular functions, disjoint issue pools,
built-in validation, and `DATA_GENERATION_NOTES.md`.

**Actual run (seed 42):**

| Metric | Value |
| ------ | ----- |
| customers | 10,000 rows (775.8 KB) |
| products | 500 rows (23.8 KB) |
| orders | 100,000 rows (6,320 KB) |
| injections | 460 |
| validation | PASSED |

### YOUR EVALUATION

**✓ ACCEPTED**

- All explicit issue counts match
- Reproducible with seed 42
- 460 vs 700 gap documented
- Invalid FK IDs: 90001+, 9001+

**FINAL DECISION:** Use as `generate_sample_data.py`

---

## Prompt 4: Run Data Generator

### PROMPT SENT

> ```bash
> python3 -m pip install -r requirements.txt
> python3 src/data_generation/generate_sample_data.py
> ```

### AI RESPONSE SUMMARY

Installed faker/pandas; ran generator successfully. Validation PASSED.

### YOUR EVALUATION

**✓ ACCEPTED**

---
