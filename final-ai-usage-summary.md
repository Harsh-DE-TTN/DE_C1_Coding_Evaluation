# Final AI Usage Summary

**Project:** E-commerce Medallion Architecture Pipeline  
**AI Tool:** Cursor (Claude)  
**Approach:** Incremental, layer-by-layer implementation with saved prompt history

---

## Scope of AI assistance

| Area | AI contribution | Human oversight |
| ---- | --------------- | --------------- |
| Project structure | Generated folder layout and cursor-workflow files | Approved structure before implementation |
| Architecture | Two design iterations; v2 approved | Rolled back v1; accepted v2 + diagram |
| Data generation | Full generator script + 460 DQ injections | Verified counts and CSV output |
| Bronze layer | PySpark ingest scripts | Removed local run after failures |
| Silver layer | 5 DQ modules + orchestrator + tests | Reviewed thresholds and FK order |
| Gold layer | 4 SQL aggregations + orchestrator + tests | Confirmed revenue rules and segment logic |
| Dashboard | 5 SQL queries + guide | No UI — SQL only |
| Documentation | README, design notes, prompt history | Filled reflection and candidate info |

---

## Prompt history location

**Master index:** [ai-prompts/documentation.md](ai-prompts/documentation.md)

| File | Content |
| ---- | ------- |
| `ai-prompts/documentation.md` | Project setup + architecture + master index |
| `ai-prompts/data-generation.md` | Sample data prompts |
| `ai-prompts/bronze-layer.md` | Bronze ingestion prompts |
| `ai-prompts/silver-layer.md` | Silver validation prompts |
| `ai-prompts/gold-layer.md` | Gold aggregation prompts |
| `ai-prompts/dashboard.md` | Dashboard SQL prompts |
| `ai-prompts/debugging.md` | Rollback, local-run removal, fixes |
| `ai-prompts/documentation.md` | Skeleton, structure, prompt history setup |

---

## Key decisions driven by AI + human review

1. **Databricks-only Bronze** — after local PySpark/Delta failures
2. **460 explicit DQ injections** — not ~700; documented clearly
3. **Silver FK order** — customers → products → orders
4. **Gold revenue** — Completed orders only; High-Value at $5,000
5. **No dashboard UI** — SQL queries for Databricks Dashboard attachment

---

## Validation of AI output

- **17/17** prompt phases logged in `ai-prompts/documentation.md`
- Design artifacts: `requirements-analysis.md`, `data-model.md`, `data-quality-strategy.md`, `design-notes.md`

---

## Honest assessment

AI accelerated implementation significantly — especially repetitive PySpark patterns, SQL aggregations, and documentation. Human judgment was essential for:

- Approving architecture before coding
- Deciding to drop local Bronze execution
- Verifying DQ thresholds match business intent
- Ensuring prompt history completeness for submission requirements
