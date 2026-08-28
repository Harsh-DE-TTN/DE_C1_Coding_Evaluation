# AI Prompts — Debugging & Iterations

## Prompt 1: Roll Back Architecture Design

### PROMPT SENT

> roll back last prompt changes

### AI RESPONSE SUMMARY

Restored `design-notes.md`, `data-model.md`, `tool-specific/cursor-workflow/spec.md`
to last git commit. Removed medallion architecture canvas. Left untracked
`ai-prompts/architecture.md` unchanged.

### YOUR EVALUATION

**✓ ACCEPTED** — User wanted to undo design-doc write before re-approving architecture

---

## Prompt 2: Remove Local Bronze Run

### PROMPT SENT

> remove all files for local run

### AI RESPONSE SUMMARY

See [bronze-layer.md](bronze-layer.md#prompt-2-remove-local-run-files).

Removed local PySpark config, delta-spark deps, and local run documentation.

### YOUR EVALUATION

**✓ ACCEPTED**

---

## Prompt 3: Fix Architecture Image Link

### PROMPT SENT

> fix image path in architecture.md

### AI RESPONSE SUMMARY

See [architecture.md](architecture.md#prompt-4-fix-architecture-image-path).

### YOUR EVALUATION

**✓ ACCEPTED**

---

## Known issues / notes

| Issue | Resolution |
| ----- | ---------- |
| Local PySpark Delta classpath error | Removed local run; use Databricks cluster |
| ~700 vs 460 DQ injections | Documented in DATA_GENERATION_NOTES.md — only 460 explicit injections |
| File sizes exceed targets | Documented — Faker produces long strings |

---
