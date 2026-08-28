# Reflection

## What I Built

An end-to-end e-commerce Medallion pipeline:

- Sample data generator with 460 intentional DQ defects
- Bronze CSV → Delta ingestion (Databricks)
- Silver validation with 5 check categories and quarantine tables
- Gold aggregations (4 business tables)
- Dashboard SQL queries (5 visualizations)
- Unit tests for Silver and Gold logic (Databricks CI)
- Full AI prompt history and design artifacts

## How I Used AI (Across the Lifecycle)

| Phase | AI role |
| ----- | ------- |
| Planning | Architecture design, data model, folder structure |
| Implementation | Layer-by-layer code generation (data gen → bronze → silver → gold → dashboard) |
| Debugging | Rollback, local-run removal, test fixes |
| Documentation | README, layer guides, prompt history, design notes |
| Testing | PySpark unit test scaffolding |

Work was done incrementally — one layer at a time per user instruction — with prompt history saved in `ai-prompts/`.

## What AI Helped With Most

- **Boilerplate speed:** PySpark ingest, Silver check modules, Gold SQL, and test scaffolding
- **Consistency:** Naming conventions, quality column patterns, orchestrator structure
- **Documentation:** Layer READMEs, data model Mermaid diagrams, DQ strategy tables

## What AI Got Wrong

- **Local Bronze execution:** Initial local PySpark setup failed; removed after user direction
- **Injection count ambiguity:** Early references to ~700 defects vs actual 460 explicit injections
- **Minor test issues:** PySpark syntax and schema mismatches in Silver tests (fixed iteratively)

## How I Validated AI Output

1. Ran data generator and verified self-check counts
2. Databricks pipeline runs and `silver.data_quality_report` inspection
3. Reviewed SQL and check logic against `data-quality-strategy.md`
4. Manual review of prompt history and design docs for consistency

## What I Would Improve Next

- Integration test on Databricks (full Bronze → Silver → Gold run in CI)
- Incremental Bronze ingest (append mode) instead of full overwrite
- Data quality alerting on threshold breach
- Dashboard UI in Databricks with the SQL queries wired up

## Reusable Workflow

1. Define architecture and data model first (with AI), get approval
2. Implement one layer at a time; save prompts after each phase
3. Use Databricks for Spark/Delta layers; local Python for data generation
4. Document intentional defects and thresholds before Silver implementation
5. Keep `ai-prompts/documentation.md` as the master index for submission
