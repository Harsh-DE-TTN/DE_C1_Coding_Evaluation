#!/usr/bin/env python3
"""Bronze ingestion entry point: products.csv → bronze.bronze_products."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest_all import run_ingest  # noqa: E402

if __name__ == "__main__":
    sys.exit(run_ingest("products"))
