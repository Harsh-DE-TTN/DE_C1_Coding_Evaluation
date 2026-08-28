# AI Prompts — Documentation & Project Setup

## Master prompt index

| # | Phase | Location | Status |
| - | ----- | -------- | ------ |
| 1 | Project skeleton | [Phase 1](#phase-1--project-skeleton) | Done |
| 2 | Detailed repo structure | [Phase 2](#phase-2--detailed-repo-structure) | Done |
| 3 | Cursor workflow | [Phase 3](#phase-3--cursor-workflow-setup) | Done |
| 4 | Requirements analysis | [Phase 4](#phase-4--requirements-analysis) | Done |
| 5 | Architecture v1 (rolled back) | [Architecture prompts](#prompt-1-initial-architecture-design) | Rolled back |
| 6 | Architecture v2 (approved) | [Architecture prompts](#prompt-2-full-architecture-design-approved) | Accepted |
| 7 | Architecture image | [Architecture prompts](#prompt-3-architecture-diagram-image) | Done |
| 8 | Data model | [Architecture prompts](#prompt-4-data-model) | Accepted |
| 9 | Sample data generation | [data-generation.md](data-generation.md) | Done |
| 10 | Bronze ingestion | [bronze-layer.md](bronze-layer.md) | Done |
| 11 | Remove local run | [bronze-layer.md](bronze-layer.md) | Done |
| 12 | Prompt history | [Phase 12](#phase-12--prompt-history) | Done |
| 13 | Silver validation | [silver-layer.md](silver-layer.md) | Done |
| 14 | Gold aggregations | [gold-layer.md](gold-layer.md) | Done |
| 15 | Dashboard SQL | [dashboard.md](dashboard.md) | Done |
| 16 | Debugging | [debugging.md](debugging.md) | Done |
| 17 | Checklist / docs | [Phase 17](#phase-17--full-checklist--documentation-completion) | Done |

---

### PROMPT SENT

> create this file structure databricks-medallion-pipeline/
>
> [Full tree: README.md, candidate-info.md, tool-workflow.md, requirements-analysis.md,
> design-notes.md, data-model.md, data-quality-strategy.md, src/, data/, database/,
> debugging-notes.md, reflection.md, final-ai-usage-summary.md, ai-prompts/]

### AI RESPONSE SUMMARY

Created root markdown placeholders, `src/` layer folders, `data/*.csv` placeholders,
`database/`, and `ai-prompts/`. Used `.gitkeep` for empty directories.

### YOUR EVALUATION

**✓ ACCEPTED** — Skeleton matches requested layout.

---



## Phase 2 — Detailed repo structure



### PROMPT SENT

> Fix folder structure like this [detailed image with all src/bronze, src/silver,
> src/gold, src/dashboard, src/data_generation files, database/, ai-prompts/*.md]



### AI RESPONSE SUMMARY

Added all missing Python/SQL/Markdown files per image. Removed `.gitkeep` files
where real files were added.

### YOUR EVALUATION

**✓ ACCEPTED** — Structure matches exercise specification.

---



## Phase 3 — Cursor workflow setup



### PROMPT SENT

> add this structure also tool-specific/cursor-workflow/ with:
>
> - project-context.md
> - spec.md
> - cursor-rules-or-instructions.md
>
> add these demo lines also
>
> add task-breakdown.md — Tasks as you defined them to Cursor



### AI RESPONSE SUMMARY

Created Cursor workflow notes (now consolidated in `tool-workflow.md`).

### YOUR EVALUATION

**✓ ACCEPTED**

---



## Phase 4 — Requirements analysis



### PROMPT SENT

> (Implicit via requirements-analysis.md population and Phase 2 documentation prompt)



### AI RESPONSE SUMMARY

`requirements-analysis.md` created with problem statement, functional/non-functional
requirements, edge cases, and clarifications needed.

### YOUR EVALUATION

**✓ ACCEPTED** — Committed on `feature_1`.

---



## Phase 5 — Architecture design (rolled back)



### PROMPT SENT

> Act as a Senior Databricks Data Engineer and Medallion Architecture specialist.
> Design the Bronze → Silver → Gold architecture...
> Do not write code until the design is approved.



### AI RESPONSE SUMMARY

Wrote `design-notes.md`, `data-model.md`, canvas diagram. User requested rollback.

### YOUR EVALUATION

**✗ ROLLED BACK** — User said "roll back last prompt changes". Files restored to
last commit; canvas removed.

---



## Phase 6 — Architecture design (approved)

See [Architecture prompts](#prompt-2-full-architecture-design-approved) below.

Output: architecture design in chat, Mermaid diagrams in `design-notes.md` and `data-model.md`.

---



## Phase 7 — Architecture image

See [Architecture prompts](#prompt-3-architecture-diagram-image) below.

---



## Phase 8 — Data model

See [Architecture prompts](#prompt-4-data-model) below.

Output: `data-model.md`

---



## Phase 9 — Sample data generation

See [data-generation.md](data-generation.md).

Output: `generate_sample_data.py`, `DATA_GENERATION_NOTES.md`, `data/*.csv`

---



## Phase 10 — Bronze layer

See [bronze-layer.md](bronze-layer.md).

Output: `src/bronze/` scripts (`ingest_all.py` contains shared ingest utilities)

---



## Phase 11 — Remove local run

See [bronze-layer.md](bronze-layer.md#prompt-2-remove-local-run-files).

---



## Phase 12 — Prompt history



### PROMPT SENT

> add all prompt history in .md files



### AI RESPONSE SUMMARY

Created master prompt index in this file and updated all `ai-prompts/*.md` files.

### YOUR EVALUATION

**✓ ACCEPTED**

---



## Phase 13 — Silver layer validation

See [silver-layer.md](silver-layer.md).

Output: `src/silver/` scripts, updated `data-quality-strategy.md`

---



## Phase 14 — Gold layer aggregations

See [gold-layer.md](gold-layer.md).

Output: `src/gold/` SQL scripts and `create_gold_tables.py`

---

## Phase 15 — Dashboard SQL queries

See [dashboard.md](dashboard.md).

Output: `src/dashboard/dashboard_queries.sql`, `DASHBOARD_GUIDE.md`

---

## Phase 16 — Debugging & iterations

See [debugging.md](debugging.md).

Output: `debugging-notes.md`, Bronze Databricks-only decision, test fixes

---

## Phase 17 — Full checklist / documentation completion

### PROMPT SENT

> Verify all submission checklist items are implemented (README, schema, artifacts, tests, prompt history)

### AI RESPONSE SUMMARY

Filled `README.md`, `database/`, `design-notes.md`, `debugging-notes.md`, `reflection.md`,
`final-ai-usage-summary.md`, `tool-workflow.md`.

### YOUR EVALUATION

**✓ ACCEPTED** — All checklist items mapped to repository files

---

## Phase index (quick reference)

```text
1  Skeleton
2  Detailed structure
3  Cursor workflow
4  Requirements
5  Architecture v1 (rolled back)
6  Architecture v2 (approved)
7  Architecture image
8  Data model
9  Data generation
10 Bronze ingestion
11 Remove local run
12 Prompt history
13 Silver validation
14 Gold aggregations
15 Dashboard SQL queries
16 Debugging
17 Full checklist / documentation
```


---

# AI Prompts — For Architecture

## Prompt 1: Initial For Architecture

### PROMPT SENT

> Act as a Senior Databricks Data Engineer and Medallion Architecture specialist. Design the Bronze → Silver → Gold architecture for this e-commerce pipeline.
>
> For each layer, define:
>
> - Purpose
> - Input
> - Output
> - Transformations
> - Tables
> - Schema
> - Data quality rules
> - Error handling
> - Expected data volume
> - Why this design is appropriate
>
> Keep Bronze raw and unchanged, perform validation and cleansing in Silver, and create business-ready aggregations in Gold.
>
> Do not write code until the design is approved.



### AI RESPONSE SUMMARY

[Cursor generated E-commerce Medallion Architecture]

### YOUR EVALUATION

**✓ What was good:**

- Design

**✗ What needed fixing:**

- Flow of design

**△ Missing:**

- Quality checks and flows

---



# Iteration 1: Adding Quality Issues



### PROMPT SENT

> Act as a Senior Data Architect and Senior Databricks Data Engineer with 10+ years of experience in designing production-grade data platforms.
>
> I am building an e-commerce data pipeline using Databricks Medallion Architecture.
>
> ## DATA SUMMARY
>
> The system receives daily sales data from three CSV source files:
>
> ### 1. customers.csv
>
> - Approximately 10,000 records
> - Primary Key: `customer_id`
> - Columns:
>   - `customer_id` INT
>   - `customer_name` STRING
>   - `email` STRING
>   - `country` STRING
>   - `signup_date` DATE
>   - `customer_segment` STRING (Premium/Standard/Basic)
>   - `lifetime_value` DECIMAL
>
> ### 2. orders.csv
>
> - Approximately 100,000 records
> - Primary Key: `order_id`
> - Foreign Keys:
>   - `customer_id` → `customers.customer_id`
>   - `product_id` → `products.product_id`
> - Columns:
>   - `order_id` INT
>   - `customer_id` INT
>   - `order_date` DATE
>   - `product_id` INT
>   - `quantity` INT
>   - `unit_price` DECIMAL
>   - `total_amount` DECIMAL
>   - `order_status` STRING (Pending/Completed/Cancelled)
>   - `payment_date` DATE, nullable
>
> ### 3. products.csv
>
> - Approximately 500 records
> - Primary Key: `product_id`
> - Columns:
>   - `product_id` INT
>   - `product_name` STRING
>   - `category` STRING
>   - `price` DECIMAL
>   - `cost` DECIMAL
>   - `stock_quantity` INT
>   - `reorder_level` INT
>
> ## INTENTIONAL DATA QUALITY ISSUES
>
> ### customers.csv
>
> - 50 rows with NULL email
> - 10 rows with duplicate `customer_id`
>
> ### orders.csv
>
> - 100 rows with NULL `customer_id`
> - 200 rows with NULL `product_id`
> - 50 rows with `customer_id` values that do not exist in customers
> - 30 rows with `product_id` values that do not exist in products
> - 20 duplicate `order_id` rows
>
> ## BUSINESS OBJECTIVE
>
> The company wants to transform raw e-commerce data into trusted, business-ready analytics data.
>
> Required pipeline:
>
> ```text
> Source CSV Files
>       ↓
>     Bronze
>       ↓
>     Silver
>       ↓
>      Gold
>       ↓
>   Dashboard
> ```
>
> ## BRONZE REQUIREMENTS
>
> - Ingest all three CSV files into Databricks.
> - Preserve raw source data.
> - Do not perform business transformations.
> - Handle schemas and data types.
> - Capture ingestion metadata such as ingestion timestamp and source.
> - Store data in appropriate Bronze tables.
>
> ## SILVER REQUIREMENTS
>
> Clean, validate, and prepare trusted data.
>
> ### Required Quality Checks
>
> #### 1. Completeness
>
> - Check NULL values in critical fields.
>
> #### 2. Uniqueness
>
> - Detect duplicate `customer_id`.
> - Detect duplicate `order_id`.
>
> #### 3. Referential Integrity
>
> - Validate `customer_id` against customers.
> - Validate `product_id` against products.
>
> #### 4. Data Type / Business Rule Validation
>
> - Validate expected data types and reasonable business values.
>
> **Important:**
>
> - Do NOT silently delete bad records.
> - Flag invalid records with appropriate quality columns/status.
> - Generate data-quality metrics showing the percentage of records passing each check.
>
> ## GOLD REQUIREMENTS
>
> Create business-ready aggregation tables:
>
> ### 1. Sales by Product
>
> - `product_id`
> - `product_name`
> - `category`
> - `total_orders`
> - `total_revenue`
> - `avg_order_value`
>
> ### 2. Revenue by Customer
>
> - `customer_id`
> - `customer_name`
> - `customer_segment`
> - `total_orders`
> - `total_revenue`
> - `avg_order_value`
> - `lifetime_value_actual`
>
> ### 3. Customer Segmentation
>
> - `segment_type`
> - `customer_count`
> - `avg_revenue`
> - `total_revenue`
>
> ## DASHBOARD REQUIREMENTS
>
> Create data suitable for a Databricks SQL Dashboard containing at least:
>
> 1. Top 10 products by revenue — Bar Chart
> 2. Customer revenue distribution — Histogram
> 3. Customer segmentation — Pie Chart
>
> ## TECHNOLOGY
>
> - Databricks
> - PySpark
> - Python
> - SQL
> - Delta Lake
> - Databricks SQL Dashboard
>
> ## ARCHITECTURE EXPECTATIONS
>
> Design a scalable, maintainable, testable, and production-oriented architecture.
>
> Include:
>
> 1. High-Level Architecture
> 2. End-to-End Data Flow
> 3. Bronze Layer Design
> 4. Silver Layer Design
> 5. Gold Layer Design
> 6. Data Quality Architecture
> 7. Data Model and Table Relationships
> 8. Error Handling
> 9. Logging and Monitoring
> 10. Testing Strategy
> 11. Security Considerations
> 12. Dashboard Data Flow
>
> For every major architectural decision explain:
>
> - **WHAT** are we doing?
> - **WHY** are we doing it?
> - **HOW** will it work?
> - What are the trade-offs?
> - What assumptions are being made?
>
> Also identify:
>
> - Primary keys
> - Foreign keys
> - Table relationships
> - Data flow
> - Transformation boundaries
> - Where data quality checks happen
> - How invalid records are handled
> - How quality metrics are generated
> - How the pipeline can be tested
>
> ## IMPORTANT WORKING RULES
>
> 1. Do NOT generate implementation code yet.
> 2. First design the architecture.
> 3. Do not invent requirements that are not provided above.
> 4. Clearly identify assumptions.
> 5. Follow Medallion Architecture principles.
> 6. Keep Bronze raw and Silver trusted/validated.
> 7. Gold should contain business-ready analytics.
> 8. Do not silently discard bad records.
> 9. Consider scalability and maintainability.
> 10. Explain your reasoning behind important design decisions.
>
> ## OUTPUT FORMAT
>
> Start with:
>
> ```text
> ## 1. Architecture Overview
> ```
>
> Then provide:
>
> ```text
> ## 2. Architecture Diagram
> ```
>
> Use a Mermaid diagram.
>
> ```text
> ## 3. Data Flow
>
> ## 4. Data Model
>
> ## 5. Bronze Layer Design
>
> ## 6. Silver Layer Design
>
> ## 7. Gold Layer Design
>
> ## 8. Data Quality Architecture
>
> ## 9. Error Handling
>
> ## 10. Monitoring and Logging
>
> ## 11. Testing Strategy
>
> ## 12. Security Considerations
>
> ## 13. Design Decisions and Trade-offs
>
> ## 14. Assumptions
>
> ## 15. Recommended Repository Components
> ```
>
> At the end, provide a concise summary of the final architecture.
>
> **Generate an image.**
>
> Do not generate Python, PySpark, or SQL implementation code until I explicitly ask for it.



### AI RESPONSE SUMMARY

Architecture Diagram and all required details.

ecommerce-medallion-architecture

### YOUR EVALUATION

**✓ ACCEPTED**

- Modifications are correct.
- Intentional quality issues are included and clearly commented.
- Architecture flow and quality requirements are now covered.

**✗ What needed fixing:**

- None.

**△ Missing:**

- None.

---



# FINAL DECISION

**The proposed architecture is approved and will be implemented.**


## Prompt 1: Initial For Design Model



### PROMPT SENT
" Act as a Senior Data Modeler and Databricks Data Engineer.

Based on the approved requirements and Medallion Architecture, define the data model for my e-commerce pipeline.

Source tables:

customers:
customer_id PK, customer_name, email, country, signup_date, customer_segment, lifetime_value

orders:
order_id PK, customer_id FK, order_date, product_id FK, quantity, unit_price, total_amount, order_status, payment_date

products:
product_id PK, product_name, category, price, cost, stock_quantity, reorder_level

Define the data model for:
1. Source
2. Bronze
3. Silver
4. Gold

For each table include:
- Purpose
- Grain
- Columns and data types
- Primary/foreign keys
- Relationships and cardinality
- Important constraints
- Data-quality considerations

For Silver, explain handling of NULLs, duplicates, invalid foreign keys, and quality status/flags. Do not silently delete bad records.

For Gold, define:
- sales_by_product
- revenue_by_customer
- customer_segmentation

For each Gold table include its grain, source tables, join keys, dimensions, measures, and business purpose.

Create `data-model.md` with:

# Data Model
## 1. Overview
## 2. Source Data Model
## 3. Entity Relationships
## 4. Bronze Data Model
## 5. Silver Data Model
## 6. Gold Data Model
## 7. Data Quality & Constraints
## 8. Data Lineage
## 9. Design Decisions
## 10. Assumptions

Include a Mermaid ER diagram and data lineage diagram.

IMPORTANT:
- Do not generate code.
- Do not invent requirements.
- Clearly mark assumptions.
- Keep the model consistent with Bronze → Silver → Gold.
- Keep the documentation concise and production-oriented. "


### AI RESPONSE SUMMARY
Generate Data model in ER format in data-model.md file

### YOUR EVALUATION
It is good 

**✓ ACCEPTED**