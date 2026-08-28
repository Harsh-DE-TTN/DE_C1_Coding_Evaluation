#!/usr/bin/env python3
"""Bronze ingestion entry point: orders.csv → bronze.bronze_orders."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_src = os.environ.get("PIPELINE_SRC_ROOT")
if _src:
    root = Path(_src).resolve()
elif sys.path and sys.path[0]:
    p0 = Path(sys.path[0]).resolve()
    root = p0.parent if p0.name == "bronze" and (p0.parent / "silver").is_dir() else p0
else:
    try:
        root = Path(__file__).resolve().parent.parent
    except NameError:
        raise RuntimeError("Set PIPELINE_SRC_ROOT='/Workspace/Repos/<user>/<repo>/src'") from None

for entry in (root, root / "bronze"):
    s = str(entry)
    if s not in sys.path:
        sys.path.insert(0, s)

from ingest_all import run_ingest  # noqa: E402

if __name__ == "__main__":
    sys.exit(run_ingest("orders"))
