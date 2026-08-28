# E-commerce Medallion Architecture Pipeline

AI Capability Exercise — Databricks Medallion pipeline (Bronze → Silver → Gold → Dashboard SQL).

## Repository structure

```text
databricks-medallion-pipeline/
├── README.md
├── candidate-info.md
├── tool-workflow.md
├── requirements-analysis.md
├── design-notes.md
├── data-model.md
├── data-quality-strategy.md
│
├── src/
│   ├── data_generation/
│   │   ├── generate_sample_data.py
│   │   └── DATA_GENERATION_NOTES.md
│   ├── bronze/
│   │   ├── 01_ingest_customers.py
│   │   ├── 02_ingest_orders.py
│   │   ├── 03_ingest_products.py
│   │   └── ingest_all.py
│   ├── silver/
│   │   ├── 01_quality_completeness.py
│   │   ├── 02_quality_uniqueness.py
│   │   ├── 03_quality_type_validation.py
│   │   ├── 04_quality_referential_integrity.py
│   │   ├── 05_quality_business_logic.py
│   │   └── create_silver_tables.py
│   ├── gold/
│   │   ├── 01_sales_by_product.sql
│   │   ├── 02_revenue_by_customer.sql
│   │   ├── 03_daily_weekly_trends.sql
│   │   ├── 04_customer_segmentation.sql
│   │   └── create_gold_tables.py
│   └── dashboard/
│       ├── dashboard_queries.sql
│       └── DASHBOARD_GUIDE.md
│
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   └── products.csv
│
├── database/
│   ├── schema.sql
│   ├── seed-data-notes.md
│   └── setup-notes.md
│
├── debugging-notes.md
├── reflection.md
├── final-ai-usage-summary.md
│
└── ai-prompts/
    ├── data-generation.md
    ├── bronze-layer.md
    ├── silver-layer.md
    ├── gold-layer.md
    ├── dashboard.md
    ├── debugging.md
    └── documentation.md
```

---

## Quick start

### 1. Generate sample data (local)

```bash
python3 -m pip install faker pandas
python3 src/data_generation/generate_sample_data.py
```

### 2. Upload CSVs to Databricks

Upload `data/*.csv` to your ingest path and set:

```bash
export BRONZE_INPUT_PATH="/Volumes/<catalog>/<schema>/<volume>/data"
```

### 3. Create schemas

Run `database/schema.sql` in Databricks SQL.

### 4. Run full ETL pipeline (Databricks)

**All stages in one command** (each Bronze CSV ingested separately):

```python
import os
os.environ["BRONZE_INPUT_PATH"] = "/Volumes/<catalog>/<schema>/<volume>/data"
os.environ["DASHBOARD_MATERIALIZE"] = "true"  # optional
%run ./src/run_full_etl_pipeline
```

**Or run Bronze one file at a time:**

```bash
python src/bronze/01_ingest_customers.py
python src/bronze/02_ingest_orders.py
python src/bronze/03_ingest_products.py
```

Or:

```bash
python src/run_full_etl_pipeline.py
```

**Medallion only** (bronze → silver → gold, CSVs must already exist):

```bash
python src/run_medallion_pipeline.py
```

See [database/job-setup-notes.md](database/job-setup-notes.md) for multi-task Databricks Job setup.

### 5. Dashboard

Use queries from `src/dashboard/dashboard_queries.sql` in Databricks SQL Editor.

---

## Configuration

| Variable | Default | Layer |
| -------- | ------- | ----- |
| `BRONZE_INPUT_PATH` | *(required)* | Bronze |
| `BRONZE_DATABASE` | `bronze` | Bronze |
| `SILVER_DATABASE` | `silver` | Silver |
| `GOLD_DATABASE` | `gold` | Gold |
| `GOLD_HIGH_VALUE_REVENUE_THRESHOLD` | `5000.00` | Gold |

---

## Documentation

| Document | Purpose |
| -------- | ------- |
| [design-notes.md](design-notes.md) | Architecture summary |
| [data-model.md](data-model.md) | Source → Bronze → Silver → Gold |
| [data-quality-strategy.md](data-quality-strategy.md) | DQ checks and thresholds |
| [ai-prompts/documentation.md](ai-prompts/documentation.md) | **Full prompt history** |
| [database/setup-notes.md](database/setup-notes.md) | Databricks setup |

---

## License / attribution

AI Capability Exercise submission — see `candidate-info.md`.
