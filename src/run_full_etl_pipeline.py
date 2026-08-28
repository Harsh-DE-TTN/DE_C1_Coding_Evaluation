#!/usr/bin/env python3
"""
Full ETL pipeline: Data Generation → Bronze → Silver → Gold → Dashboard.

Stages (in order):
  1. DATA_GENERATION — create sample CSVs with intentional DQ issues
  2. BRONZE_CUSTOMERS  — ingest customers.csv
  3. BRONZE_ORDERS     — ingest orders.csv
  4. BRONZE_PRODUCTS   — ingest products.csv
  5. SILVER            — validate, quarantine, DQ report
  6. GOLD              — business aggregations
  7. DASHBOARD         — execute Gold dashboard SQL queries

Environment variables:
  BRONZE_INPUT_PATH       — CSV directory (required for Bronze+; also used for data gen output)
  ETL_DATA_OUTPUT_PATH    — override CSV output path for data generation (defaults to BRONZE_INPUT_PATH)
  SKIP_DATA_GENERATION    — set true to skip stage 1 if CSVs already exist
  SKIP_DASHBOARD          — set true to skip stage 5
  DASHBOARD_MATERIALIZE   — set true to write dashboard results to gold.dashboard_* tables
  BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE — schema names (optional)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable

# Bootstrap src/ on sys.path before layer imports (Databricks %run safe).
_src = os.environ.get("PIPELINE_SRC_ROOT")
if _src:
    _bootstrap = str(Path(_src).resolve())
    if _bootstrap not in sys.path:
        sys.path.insert(0, _bootstrap)
elif not any(
    Path(p).resolve().joinpath("pipeline_paths.py").is_file() for p in sys.path if p
):
    if sys.path and sys.path[0]:
        _p0 = Path(sys.path[0]).resolve()
        if (_p0 / "pipeline_paths.py").is_file():
            sys.path.insert(0, str(_p0))
        elif (_p0.parent / "pipeline_paths.py").is_file():
            sys.path.insert(0, str(_p0.parent))

from pipeline_paths import configure_layer_paths  # noqa: E402

configure_layer_paths("data_generation", "bronze", "silver", "gold", "dashboard")

from generate_sample_data import resolve_output_dir, run_data_generation  # noqa: E402
from ingest_all import run_ingest  # noqa: E402
from create_silver_tables import run_silver_pipeline  # noqa: E402
from create_gold_tables import run_gold_pipeline  # noqa: E402
from run_dashboard_queries import run_dashboard_queries  # noqa: E402

LOGGER = logging.getLogger("full_etl_pipeline")

REQUIRED_CSV_FILES = {
    "BRONZE_CUSTOMERS": "customers.csv",
    "BRONZE_ORDERS": "orders.csv",
    "BRONZE_PRODUCTS": "products.csv",
}


def _is_truthy(value: str | None) -> bool:
    return (value or "").lower() in ("1", "true", "yes")


def _validate_environment() -> Path:
    data_path = resolve_output_dir()
    if not os.environ.get("BRONZE_INPUT_PATH") and not os.environ.get("ETL_DATA_OUTPUT_PATH"):
        # Local default: write/read from repo data/
        os.environ.setdefault("BRONZE_INPUT_PATH", str(data_path))
        LOGGER.info("BRONZE_INPUT_PATH not set — using %s", data_path)
    elif os.environ.get("ETL_DATA_OUTPUT_PATH") and not os.environ.get("BRONZE_INPUT_PATH"):
        os.environ["BRONZE_INPUT_PATH"] = os.environ["ETL_DATA_OUTPUT_PATH"]

    bronze_path = Path(os.environ["BRONZE_INPUT_PATH"])
    return bronze_path


def _csvs_exist(data_dir: Path) -> bool:
    return all((data_dir / name).exists() for name in REQUIRED_CSV_FILES.values())


def _csv_exists(data_dir: Path, stage_name: str) -> bool:
    filename = REQUIRED_CSV_FILES.get(stage_name)
    return bool(filename and (data_dir / filename).exists())


def run_full_etl_pipeline() -> int:
    """Execute the complete ETL pipeline end to end."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    started_at = datetime.utcnow()
    data_dir = _validate_environment()

    stages: list[tuple[str, Callable[[], int]]] = []

    if not _is_truthy(os.environ.get("SKIP_DATA_GENERATION")):
        stages.append(("DATA_GENERATION", lambda: run_data_generation(data_dir)))
    elif not _csvs_exist(data_dir):
        LOGGER.error("SKIP_DATA_GENERATION=true but CSVs missing in %s", data_dir)
        return 1
    else:
        LOGGER.info("Skipping DATA_GENERATION — CSVs present in %s", data_dir)

    stages.extend(
        [
            ("BRONZE_CUSTOMERS", lambda: run_ingest("customers")),
            ("BRONZE_ORDERS", lambda: run_ingest("orders")),
            ("BRONZE_PRODUCTS", lambda: run_ingest("products")),
            ("SILVER", run_silver_pipeline),
            ("GOLD", run_gold_pipeline),
        ]
    )

    if not _is_truthy(os.environ.get("SKIP_DASHBOARD")):
        stages.append(("DASHBOARD", run_dashboard_queries))

    LOGGER.info("START Full ETL pipeline | stages=%s | data_path=%s", [s[0] for s in stages], data_dir)

    for stage_name, stage_fn in stages:
        LOGGER.info("=" * 60)
        LOGGER.info("STAGE %s — starting", stage_name)
        LOGGER.info("=" * 60)

        if stage_name in REQUIRED_CSV_FILES and not _csv_exists(data_dir, stage_name):
            LOGGER.error(
                "Stage %s requires %s in %s",
                stage_name,
                REQUIRED_CSV_FILES[stage_name],
                data_dir,
            )
            return 1

        exit_code = stage_fn()
        if exit_code != 0:
            LOGGER.error("STAGE %s — FAILED (exit_code=%s)", stage_name, exit_code)
            LOGGER.error("END Full ETL pipeline — FAILED at %s", stage_name)
            return exit_code

        LOGGER.info("STAGE %s — SUCCESS", stage_name)

    elapsed = (datetime.utcnow() - started_at).total_seconds()
    LOGGER.info("=" * 60)
    LOGGER.info("END Full ETL pipeline — SUCCESS | elapsed_seconds=%.1f", elapsed)
    LOGGER.info("=" * 60)
    print(
        f"\nFull ETL pipeline completed in {elapsed:.1f}s.\n"
        f"  CSVs:     {data_dir}\n"
        "  Bronze:   bronze.bronze_*\n"
        "  Silver:   silver.silver_*, silver.data_quality_report\n"
        "  Gold:     gold.sales_by_product, gold.revenue_by_customer, gold.customer_segmentation\n"
        "  Dashboard: queries executed (set DASHBOARD_MATERIALIZE=true to persist gold.dashboard_*)\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(run_full_etl_pipeline())
