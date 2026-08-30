# Candidate Information
**Name:** Hardh vardhan
**Role:** SE
**Primary Technology Stack:** Python / PySpark, SQL, Databricks
**Primary AI Tool Used:** Cursor 
**Project Option Selected:** Data Pipeline (Medallion Architecture)
**Assessment Start Date:** 20 Aug 2026
**Submission Date:** 31 Aug 2026
## Tools & Environment
- Databricks: Community Edition / other
- Languages: Python, PySpark, SQL
- Libraries: PySpark, Delta Lake, pandas
- AI Tool: [Cursor / Claude]
## Setup Summary

1. **Local:** `pip install faker pandas` → run `src/data_generation/generate_sample_data.py` (or use existing `data/*.csv`).
2. **Databricks:** Upload CSVs, run `database/schema.sql`, set `BRONZE_INPUT_PATH`.
3. **Pipeline:** `src/run_full_etl_pipeline.py` (data gen → bronze → silver → gold → dashboard).