#!/usr/bin/env python3
"""
Dashboard query stage — execute Gold-layer SQL for visualization validation.

Reads query definitions from dashboard_queries.sql and runs each via Spark.
Optionally materializes results to gold.dashboard_* tables for Databricks Dashboards.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

LOGGER = logging.getLogger("dashboard_queries")

DASHBOARD_SQL_FILE = Path(__file__).resolve().parent / "dashboard_queries.sql"
GOLD_DATABASE = os.environ.get("GOLD_DATABASE", "gold")
MATERIALIZE = os.environ.get("DASHBOARD_MATERIALIZE", "false").lower() in ("1", "true", "yes")

QUERY_TABLE_MAP = {
    "top_products_by_revenue": "dashboard_top_products",
    "revenue_trend_daily": "dashboard_revenue_trend",
    "customer_segmentation_mix": "dashboard_segmentation_mix",
    "revenue_by_customer_segment": "dashboard_revenue_by_segment",
    "kpi_summary": "dashboard_kpi_summary",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("dashboard-queries").getOrCreate()


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug or "query"


def parse_dashboard_queries(sql_path: Path) -> list[tuple[str, str]]:
    """Extract named SELECT/WITH statements from dashboard_queries.sql."""
    if not sql_path.exists():
        raise FileNotFoundError(f"Dashboard SQL file not found: {sql_path}")

    content = sql_path.read_text(encoding="utf-8")
    queries: list[tuple[str, str]] = []

    for match in re.finditer(r"-- QUERY (\d+[a-z]?):\s*([^\n]+)", content, re.IGNORECASE):
        title = match.group(2).strip()
        if "optional" in title.lower():
            continue

        rest = content[match.end() :]
        sql_match = re.search(
            r"^[\s\S]*?^((?:SELECT|WITH)\b[\s\S]*?;)",
            rest,
            re.MULTILINE | re.IGNORECASE,
        )
        if not sql_match:
            continue

        sql = sql_match.group(1).strip().rstrip(";")
        if sql and not sql.lstrip().startswith("--"):
            queries.append((_slugify(title), sql))

    if not queries:
        raise ValueError(f"No executable queries found in {sql_path}")
    return queries


def _substitute_gold_schema(sql: str) -> str:
    """Allow GOLD_DATABASE override (default schema name is gold)."""
    if GOLD_DATABASE == "gold":
        return sql
    return re.sub(r"\bgold\.", f"{GOLD_DATABASE}.", sql)


def run_dashboard_queries() -> int:
    """Execute all dashboard queries; optionally save results as Delta tables."""
    configure_logging()
    spark = get_spark()
    validation_ts = datetime.utcnow()

    LOGGER.info("START Dashboard query stage | materialize=%s", MATERIALIZE)

    try:
        queries = parse_dashboard_queries(DASHBOARD_SQL_FILE)
        results_summary: list[tuple[str, int, str]] = []

        for name, sql in queries:
            sql = _substitute_gold_schema(sql)
            LOGGER.info("Running dashboard query: %s", name)
            df = spark.sql(sql)
            row_count = df.count()
            table_name = QUERY_TABLE_MAP.get(name, f"dashboard_{name}")

            if MATERIALIZE:
                full_table = f"{GOLD_DATABASE}.{table_name}"
                df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                    full_table
                )
                LOGGER.info("Materialized %s → %s (%s rows)", name, full_table, row_count)
            else:
                df.createOrReplaceTempView(f"vw_{table_name}")
                LOGGER.info("Query %s returned %s rows (temp view vw_%s)", name, row_count, table_name)

            results_summary.append((name, row_count, table_name))

        print("=" * 50)
        print("DASHBOARD QUERY SUMMARY")
        print("=" * 50)
        for name, count, table in results_summary:
            dest = f"{GOLD_DATABASE}.{table}" if MATERIALIZE else f"vw_{table}"
            print(f"  {name}: {count:,} rows → {dest}")
        print(f"\nExecuted at: {validation_ts.isoformat()}Z")
        print("=" * 50)

        LOGGER.info("END Dashboard query stage — SUCCESS | queries=%s", len(results_summary))
        return 0
    except Exception as exc:
        LOGGER.exception("END Dashboard query stage — FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(run_dashboard_queries())
