"""
Path helpers for local Python and Databricks notebook %run.

Databricks %run often leaves __file__ undefined. This module resolves src/ using,
in order:
  1. PIPELINE_SRC_ROOT environment variable
  2. __file__ (normal python execution)
  3. sys.path[0] (Databricks %run script directory)
  4. Current working directory
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def ensure_src_on_path(layer: str | None = None) -> Path:
    """Add src/ (and optional layer subdir) to sys.path. Returns src root."""
    src = _resolve_src_root()
    paths = [src]
    if layer:
        paths.append(src / layer)
    for path in reversed(paths):
        entry = str(path)
        if entry not in sys.path:
            sys.path.insert(0, entry)
    return src


def configure_layer_paths(*layers: str) -> Path:
    """Insert src/ and multiple layer subdirs onto sys.path."""
    src = _resolve_src_root()
    paths = [src] + [src / layer for layer in layers]
    for path in reversed(paths):
        entry = str(path)
        if entry not in sys.path:
            sys.path.insert(0, entry)
    return src


def resolve_src_root() -> Path:
    """Return src/ without modifying sys.path."""
    return _resolve_src_root()


def resolve_repo_root() -> Path:
    """Return repository root (parent of src/)."""
    src = _resolve_src_root()
    return src.parent if src.name == "src" else src


def resolve_layer_dir(layer: str) -> Path:
    """Return a layer directory such as src/dashboard."""
    return _resolve_src_root() / layer


def _resolve_src_root() -> Path:
    env_root = os.environ.get("PIPELINE_SRC_ROOT")
    if env_root:
        path = Path(env_root).resolve()
        if path.is_dir():
            return path

    try:
        path = Path(__file__).resolve().parent
        if _looks_like_src_root(path):
            return path
    except NameError:
        pass

    if sys.path and sys.path[0]:
        p0 = Path(sys.path[0]).resolve()
        if _looks_like_src_root(p0):
            return p0
        if _looks_like_src_root(p0.parent):
            return p0.parent

    for candidate in (Path.cwd(), Path.cwd() / "src", Path.cwd().parent / "src"):
        resolved = candidate.resolve()
        if _looks_like_src_root(resolved):
            return resolved

    for entry in sys.path:
        if not entry:
            continue
        candidate = Path(entry).resolve()
        if _looks_like_src_root(candidate):
            return candidate

    raise RuntimeError(
        "Could not locate pipeline src/ directory.\n"
        "Before %run in a Databricks notebook, run:\n"
        "  import os\n"
        "  os.environ['PIPELINE_SRC_ROOT'] = '/Workspace/Repos/<user>/<repo>/src'"
    )


def _looks_like_src_root(path: Path) -> bool:
    return (
        path.is_dir()
        and (path / "bronze").is_dir()
        and (path / "silver").is_dir()
        and (path / "gold").is_dir()
    )
