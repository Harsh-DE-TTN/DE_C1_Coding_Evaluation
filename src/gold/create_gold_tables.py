#!/usr/bin/env python3
"""
Gold layer utilities and orchestration: Silver PASS tables → business-ready Delta aggregations.

Execution order:
  1. sales_by_product
  2. revenue_by_customer
  3. customer_segmentation (reads revenue_by_customer)

Silver rejected tables and Bronze are never used.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

_src = os.environ.get("PIPELINE_SRC_ROOT")
if _src:
    root = Path(_src).resolve()
elif sys.path and sys.path[0]:
    p0 = Path(sys.path[0]).resolve()
    root = p0.parent if p0.name == "gold" and (p0.parent / "silver").is_dir() else p0
else:
    try:
        root = Path(__file__).resolve().parent.parent
    except NameError:
        raise RuntimeError("Set PIPELINE_SRC_ROOT='/Workspace/Repos/<user>/<repo>/src'") from None

for entry in (root, root / "gold"):
    s = str(entry)
    if s not in sys.path:
        sys.path.insert(0, s)

import logging
import os
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SILVER_DATABASE = os.environ.get("SILVER_DATABASE", "silver")
GOLD_DATABASE = os.environ.get("GOLD_DATABASE", "gold")
GOLD_WRITE_MODE = os.environ.get("GOLD_WRITE_MODE", "overwrite")

SILVER_CUSTOMERS_TABLE = f"{SILVER_DATABASE}.silver_customers"
SILVER_ORDERS_TABLE = f"{SILVER_DATABASE}.silver_orders"
SILVER_PRODUCTS_TABLE = f"{SILVER_DATABASE}.silver_products"

GOLD_SALES_BY_PRODUCT_TABLE = f"{GOLD_DATABASE}.sales_by_product"
GOLD_REVENUE_BY_CUSTOMER_TABLE = f"{GOLD_DATABASE}.revenue_by_customer"
GOLD_CUSTOMER_SEGMENTATION_TABLE = f"{GOLD_DATABASE}.customer_segmentation"

# Business rule: only Completed orders contribute to Gold revenue metrics.
QUALIFYING_ORDER_STATUS = os.environ.get("GOLD_QUALIFYING_ORDER_STATUS", "Completed")

# High-Value segment threshold (total completed-order revenue per customer).
HIGH_VALUE_REVENUE_THRESHOLD = float(os.environ.get("GOLD_HIGH_VALUE_REVENUE_THRESHOLD", "5000.00"))

GOLD_SQL_DIR = Path(os.environ.get("PIPELINE_SRC_ROOT", "")).resolve() / "gold" if os.environ.get(
    "PIPELINE_SRC_ROOT"
) else None


def _gold_sql_dir() -> Path:
    if GOLD_SQL_DIR and GOLD_SQL_DIR.is_dir():
        return GOLD_SQL_DIR
    try:
        return Path(__file__).resolve().parent
    except NameError:
        if sys.path and sys.path[0]:
            p0 = Path(sys.path[0]).resolve()
            if p0.name == "gold":
                return p0
            if (p0 / "gold").is_dir():
                return p0 / "gold"
            if (p0.parent / "gold").is_dir():
                return p0.parent / "gold"
        raise RuntimeError("Set PIPELINE_SRC_ROOT='/Workspace/Repos/<user>/<repo>/src'") from None

LOGGER = logging.getLogger("gold_create")


@dataclass(frozen=True)
class GoldValidationResult:
    check_name: str
    expected: float
    actual: float
    passed: bool
    detail: str


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_spark(app_name: str = "gold-create-tables") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def ensure_gold_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")


def load_sql(filename: str) -> str:
    path = _gold_sql_dir() / filename
    if not path.exists():
        raise FileNotFoundError(f"Gold SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def format_sql(template: str) -> str:
    return template.format(
        gold_db=GOLD_DATABASE,
        silver_db=SILVER_DATABASE,
        qualifying_status=QUALIFYING_ORDER_STATUS,
        high_value_threshold=HIGH_VALUE_REVENUE_THRESHOLD,
    )


def assert_silver_tables_exist(spark: SparkSession) -> None:
    for table in (SILVER_CUSTOMERS_TABLE, SILVER_ORDERS_TABLE, SILVER_PRODUCTS_TABLE):
        if not spark.catalog.tableExists(table):
            raise ValueError(f"Required Silver table not found: {table}")


def qualifying_orders_view_sql() -> str:
    """Reusable filter — valid Silver orders that count toward Gold revenue."""
    return f"""
        SELECT *
        FROM {SILVER_ORDERS_TABLE}
        WHERE order_status = '{QUALIFYING_ORDER_STATUS}'
    """


def run_sql_file(spark: SparkSession, filename: str) -> None:
    sql = format_sql(load_sql(filename))
    LOGGER.info("Executing %s", filename)
    spark.sql(sql)


def write_table_from_sql(spark: SparkSession, sql: str, table_fqn: str) -> int:
    df = spark.sql(sql)
    df.write.format("delta").mode(GOLD_WRITE_MODE).option("overwriteSchema", "true").saveAsTable(
        table_fqn
    )
    return spark.table(table_fqn).count()


def validate_gold_tables(spark: SparkSession) -> list[GoldValidationResult]:
    """Compare Gold aggregates against Silver qualifying-order totals."""
    results: list[GoldValidationResult] = []

    silver_revenue = float(
        spark.sql(
            f"""
            SELECT COALESCE(SUM(CAST(total_amount AS DOUBLE)), 0.0) AS revenue
            FROM {SILVER_ORDERS_TABLE}
            WHERE order_status = '{QUALIFYING_ORDER_STATUS}'
            """
        ).collect()[0]["revenue"]
    )

    product_revenue = float(
        spark.table(GOLD_SALES_BY_PRODUCT_TABLE)
        .agg(F.sum("total_revenue").alias("v"))
        .collect()[0]["v"]
        or 0.0
    )
    results.append(
        GoldValidationResult(
            "sales_by_product_revenue_reconciliation",
            silver_revenue,
            product_revenue,
            abs(silver_revenue - product_revenue) < 0.01,
            "SUM(sales_by_product.total_revenue) vs Silver Completed orders",
        )
    )

    customer_revenue = float(
        spark.table(GOLD_REVENUE_BY_CUSTOMER_TABLE)
        .agg(F.sum("total_revenue").alias("v"))
        .collect()[0]["v"]
        or 0.0
    )
    results.append(
        GoldValidationResult(
            "revenue_by_customer_reconciliation",
            silver_revenue,
            customer_revenue,
            abs(silver_revenue - customer_revenue) < 0.01,
            "SUM(revenue_by_customer.total_revenue) vs Silver Completed orders",
        )
    )

    seg_customers = spark.table(GOLD_CUSTOMER_SEGMENTATION_TABLE).agg(
        F.sum("customer_count").alias("c")
    ).collect()[0]["c"]
    total_customers = spark.table(GOLD_REVENUE_BY_CUSTOMER_TABLE).count()
    results.append(
        GoldValidationResult(
            "segmentation_customer_count",
            float(total_customers),
            float(seg_customers or 0),
            int(seg_customers or 0) == total_customers,
            "SUM(segmentation.customer_count) vs revenue_by_customer rows",
        )
    )

    seg_revenue = float(
        spark.table(GOLD_CUSTOMER_SEGMENTATION_TABLE)
        .agg(F.sum("total_revenue").alias("v"))
        .collect()[0]["v"]
        or 0.0
    )
    results.append(
        GoldValidationResult(
            "segmentation_revenue_reconciliation",
            customer_revenue,
            seg_revenue,
            abs(customer_revenue - seg_revenue) < 0.01,
            "SUM(segmentation.total_revenue) vs revenue_by_customer",
        )
    )

    return results


def print_gold_summary(spark: SparkSession, validation_ts: datetime, results: list[GoldValidationResult]) -> None:
    counts = {
        "sales_by_product": spark.table(GOLD_SALES_BY_PRODUCT_TABLE).count(),
        "revenue_by_customer": spark.table(GOLD_REVENUE_BY_CUSTOMER_TABLE).count(),
        "customer_segmentation": spark.table(GOLD_CUSTOMER_SEGMENTATION_TABLE).count(),
    }
    print("=" * 50)
    print("GOLD LAYER SUMMARY")
    print("=" * 50)
    print(f"Qualifying order status: {QUALIFYING_ORDER_STATUS}")
    print(f"High-Value threshold:    {HIGH_VALUE_REVENUE_THRESHOLD}")
    print(f"Published at:            {validation_ts.isoformat()}Z")
    print()
    for table, count in counts.items():
        print(f"  {table}: {count:,} rows")
    print()
    print("Validation:")
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.check_name}: expected={r.expected:,.2f} actual={r.actual:,.2f}")
        print(f"         {r.detail}")
    print("=" * 50)

SQL_PIPELINE = (
    "01_sales_by_product.sql",
    "02_revenue_by_customer.sql",
    "03_customer_segmentation.sql",
)


def run_gold_pipeline() -> int:
    configure_logging()
    validation_ts = datetime.utcnow()
    spark = get_spark()

    LOGGER.info("START Gold pipeline — input: Silver PASS tables only")

    try:
        assert_silver_tables_exist(spark)
        ensure_gold_database(spark)

        for sql_file in SQL_PIPELINE:
            run_sql_file(spark, sql_file)
            LOGGER.info("Created table from %s", sql_file)

        # Confirm tables exist and are readable
        for table in (
            GOLD_SALES_BY_PRODUCT_TABLE,
            GOLD_REVENUE_BY_CUSTOMER_TABLE,
            GOLD_CUSTOMER_SEGMENTATION_TABLE,
        ):
            count = spark.table(table).count()
            LOGGER.info("Table %s row_count=%s", table, count)

        validation_results = validate_gold_tables(spark)
        failed = [r for r in validation_results if not r.passed]
        print_gold_summary(spark, validation_ts, validation_results)

        if failed:
            raise ValueError(
                "Gold validation failed: "
                + "; ".join(f"{r.check_name} (expected={r.expected}, actual={r.actual})" for r in failed)
            )

        LOGGER.info("END Gold pipeline — SUCCESS")
        return 0
    except Exception as exc:
        LOGGER.exception("END Gold pipeline — FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(run_gold_pipeline())
