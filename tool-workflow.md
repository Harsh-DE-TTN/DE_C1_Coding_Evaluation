# Tool Workflow

How Cursor AI was used to build this Medallion pipeline submission (Part A: AI Workflow Foundation).

---

## Workflow pattern

```text
User prompt  →  AI generates code/docs  →  User reviews  →  Save prompt to ai-prompts/  →  Next layer
```

### Phase order

1. Skeleton + folder structure
2. Architecture design (approved v2)
3. Data model (Mermaid ER + lineage)
4. Sample data generation only
5. Bronze ingestion only
6. Silver validation only
7. Gold aggregations only
8. Dashboard SQL only
9. Documentation + checklist completion

Each phase was scoped narrowly ("implement only X") to keep diffs reviewable.

---

## Prompt logging convention

Every significant prompt saved under `ai-prompts/` with:

```text
### PROMPT SENT
### AI RESPONSE SUMMARY
### YOUR EVALUATION
### FINAL DECISION
```

Master index: [ai-prompts/documentation.md](ai-prompts/documentation.md)

---

## Key project documents

| Document | Purpose |
| -------- | ------- |
| [requirements-analysis.md](requirements-analysis.md) | Functional requirements |
| [design-notes.md](design-notes.md) | Architecture design summary |
| [data-model.md](data-model.md) | Entity and lineage model |
| [data-quality-strategy.md](data-quality-strategy.md) | Silver DQ strategy |
| [debugging-notes.md](debugging-notes.md) | Issues and resolutions |
| [reflection.md](reflection.md) | Post-project reflection |
| [final-ai-usage-summary.md](final-ai-usage-summary.md) | AI usage summary |

---

## Pipeline execution workflow

| Step | Script | Runtime |
| ---- | ------ | ------- |
| Generate CSVs | `src/data_generation/generate_sample_data.py` | Local Python |
| Bronze ingest | `src/bronze/ingest_all.py` | Databricks |
| Silver validate | `src/silver/create_silver_tables.py` | Databricks |
| Gold aggregate | `src/gold/create_gold_tables.py` | Databricks |
| Dashboard SQL | `src/dashboard/dashboard_queries.sql` | Databricks SQL |

---

## Debugging workflow

1. Reproduce locally for data generation
2. For Spark/Delta issues, use Databricks cluster
3. Log resolution in `debugging-notes.md` and `ai-prompts/debugging.md`
