#!/usr/bin/env python3
"""
Medallion pipeline job: Bronze → Silver → Gold (no data generation or dashboard).

For the full ETL pipeline including data generation and dashboard queries,
use src/run_full_etl_pipeline.py instead.
"""

from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC_ROOT))

from run_full_etl_pipeline import run_full_etl_pipeline  # noqa: E402


def run_medallion_pipeline() -> int:
    import os

    os.environ.setdefault("SKIP_DATA_GENERATION", "true")
    os.environ.setdefault("SKIP_DASHBOARD", "true")
    return run_full_etl_pipeline()


if __name__ == "__main__":
    sys.exit(run_medallion_pipeline())
