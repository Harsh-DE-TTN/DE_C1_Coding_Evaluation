# Debugging Notes

Chronological log of issues encountered and resolutions during pipeline development.

---

## 1. Data generation — set/list union bug

**Symptom:** Validation in `generate_sample_data.py` failed when checking injection indices.

**Cause:** Used `set | list` union incorrectly in Python.

**Fix:** Corrected index collection to use proper set operations.

**Validation:** Generator self-check passes row counts and defect counts.

---

## 2. Bronze local run — Delta / Java gateway failures

**Symptom:** Local PySpark failed with Delta Lake classpath errors and Java gateway connection issues.

**Cause:** Local environment missing Delta JARs and compatible Spark config.

**Decision:** User requested removal of local Bronze run support.

**Fix:** Removed local PySpark config; documented Databricks-only execution in `README.md` and `design-notes.md`.

---

## 3. Silver tests — `array_contains` syntax

**Symptom:** PySpark unit tests failed on array membership checks.

**Fix:** Updated to correct PySpark `array_contains` column expression syntax.

**Result:** Silver quality logic verified on Databricks.

---

## 4. Silver tests — NULL email schema mismatch

**Symptom:** Test DataFrame schema did not match production column types for NULL email scenario.

**Fix:** Aligned test schema with Silver layer expected types.

---

## 5. Architecture image path

**Symptom:** Broken architecture image reference in prompt history.

**Fix:** Image removed; architecture documented in `design-notes.md` and `data-model.md`.

---

## 6. DQ injection count confusion (~700 vs 460)

**Symptom:** Early docs referenced ~700 defects; generator implements 460 explicit injections.

**Resolution:** Documented exact counts in `DATA_GENERATION_NOTES.md` and `seed-data-notes.md`. Only assigned injection categories count toward the 460 total.

---

## 7. File size vs row-count targets

**Symptom:** CSV files larger than approximate size targets in spec.

**Cause:** Faker generates long names, emails, and country strings.

**Resolution:** Row counts match exactly; size variance documented as acceptable.

---

## Verification checklist

| Layer | How verified |
| ----- | ------------ |
| Data gen | Generator self-validation |
| Silver | Databricks run + `data_quality_report` inspection |
| Gold | Databricks run + built-in reconciliation in `create_gold_tables.py` |
| End-to-end | Bronze row counts = Silver PASS + rejected per dataset |

---

## Related

- [ai-prompts/debugging.md](ai-prompts/debugging.md) — AI prompt log for debugging iterations
