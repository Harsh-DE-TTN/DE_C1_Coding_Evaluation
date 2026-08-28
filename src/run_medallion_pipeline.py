#!/usr/bin/env python3
"""
Medallion pipeline job: Bronze → Silver → Gold (no data generation or dashboard).

For the full ETL pipeline including data generation and dashboard queries,
use src/run_full_etl_pipeline.py instead.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_src = os.environ.get("PIPELINE_SRC_ROOT")
if _src:
    root = str(Path(_src).resolve())
    if root not in sys.path:
        sys.path.insert(0, root)
elif sys.path and sys.path[0]:
    _p0 = Path(sys.path[0]).resolve()
    if (_p0 / "pipeline_paths.py").is_file() and str(_p0) not in sys.path:
        sys.path.insert(0, str(_p0))
    elif (_p0.parent / "pipeline_paths.py").is_file() and str(_p0.parent) not in sys.path:
        sys.path.insert(0, str(_p0.parent))

from pipeline_paths import configure_layer_paths  # noqa: E402

configure_layer_paths("data_generation", "bronze", "silver", "gold", "dashboard")

from run_full_etl_pipeline import run_full_etl_pipeline  # noqa: E402


def run_medallion_pipeline() -> int:
    os.environ.setdefault("SKIP_DATA_GENERATION", "true")
    os.environ.setdefault("SKIP_DASHBOARD", "true")
    return run_full_etl_pipeline()


if __name__ == "__main__":
    sys.exit(run_medallion_pipeline())
