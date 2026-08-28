"""
Silver shared utilities and completeness checks.

Shared helpers used by all Silver quality modules (import from this file).
Completeness checks — NULL values in critical fields.
"""


from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BRONZE_DATABASE = os.environ.get("BRONZE_DATABASE", "bronze")
SILVER_DATABASE = os.environ.get("SILVER_DATABASE", "silver")
SILVER_WRITE_MODE = os.environ.get("SILVER_WRITE_MODE", "overwrite")

BRONZE_CUSTOMERS_TABLE = f"{BRONZE_DATABASE}.bronze_customers"
BRONZE_ORDERS_TABLE = f"{BRONZE_DATABASE}.bronze_orders"
BRONZE_PRODUCTS_TABLE = f"{BRONZE_DATABASE}.bronze_products"

SILVER_CUSTOMERS_TABLE = f"{SILVER_DATABASE}.silver_customers"
SILVER_ORDERS_TABLE = f"{SILVER_DATABASE}.silver_orders"
SILVER_PRODUCTS_TABLE = f"{SILVER_DATABASE}.silver_products"

SILVER_CUSTOMERS_REJECTED_TABLE = f"{SILVER_DATABASE}.silver_customers_rejected"
SILVER_ORDERS_REJECTED_TABLE = f"{SILVER_DATABASE}.silver_orders_rejected"
SILVER_PRODUCTS_REJECTED_TABLE = f"{SILVER_DATABASE}.silver_products_rejected"

DATA_QUALITY_REPORT_TABLE = f"{SILVER_DATABASE}.data_quality_report"

VALID_SEGMENTS = ("Premium", "Standard", "Basic")
VALID_ORDER_STATUSES = ("Pending", "Completed", "Cancelled")

AMOUNT_TOLERANCE = 0.01

# Expected intentional defect minimums (from sample data generation)
EXPECTED_NULL_EMAILS = 50
EXPECTED_DUP_CUSTOMER_KEYS = 10
EXPECTED_NULL_ORDER_CUSTOMER = 100
EXPECTED_NULL_ORDER_PRODUCT = 200
EXPECTED_INVALID_ORDER_CUSTOMER = 50
EXPECTED_INVALID_ORDER_PRODUCT = 30
EXPECTED_DUP_ORDER_KEYS = 20

LOGGER = logging.getLogger("silver_validate")

REPORT_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), nullable=False),
        StructField("check", StringType(), nullable=False),
        StructField("total", LongType(), nullable=False),
        StructField("passed", LongType(), nullable=False),
        StructField("failed", LongType(), nullable=False),
        StructField("pass_pct", DoubleType(), nullable=False),
        StructField("fail_pct", DoubleType(), nullable=False),
        StructField("validation_timestamp", TimestampType(), nullable=False),
    ]
)


@dataclass(frozen=True)
class CheckMetric:
    """One row of silver.data_quality_report before Spark conversion."""

    dataset: str
    check: str
    total: int
    passed: int
    failed: int
    validation_timestamp: datetime

    @property
    def pass_pct(self) -> float:
        return round((self.passed / self.total) * 100, 4) if self.total else 0.0

    @property
    def fail_pct(self) -> float:
        return round((self.failed / self.total) * 100, 4) if self.total else 0.0


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_spark(app_name: str = "silver-validate") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def ensure_silver_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")


def read_bronze_table(spark: SparkSession, table_fqn: str) -> DataFrame:
    if not spark.catalog.tableExists(table_fqn):
        raise ValueError(f"Required Bronze table not found: {table_fqn}")
    return spark.table(table_fqn)


def init_failed_checks(df: DataFrame) -> DataFrame:
    if "_failed_checks" not in df.columns:
        return df.withColumn("_failed_checks", F.array().cast(ArrayType(StringType())))
    return df


def add_check_failure(df: DataFrame, condition: Column, check_code: str) -> DataFrame:
    """Append a check code when condition is True (row fails the check)."""
    df = init_failed_checks(df)
    return df.withColumn(
        "_failed_checks",
        F.when(
            condition,
            F.array_union(F.col("_failed_checks"), F.array(F.lit(check_code))),
        ).otherwise(F.col("_failed_checks")),
    )


def metric_for_check(df: DataFrame, dataset: str, check_code: str, ts: datetime) -> CheckMetric:
    total = df.count()
    failed = df.filter(F.array_contains(F.col("_failed_checks"), check_code)).count()
    passed = total - failed
    return CheckMetric(dataset, check_code, total, passed, failed, ts)


def finalize_quality_columns(df: DataFrame, validation_ts: datetime) -> DataFrame:
    """Add quality_status, quality_check_result, quality_check_reason."""
    return (
        df.withColumn(
            "quality_check_reason",
            F.when(F.size(F.col("_failed_checks")) > 0, F.concat_ws("; ", F.col("_failed_checks"))).otherwise(
                F.lit(None)
            ),
        )
        .withColumn(
            "quality_check_result",
            F.when(F.size(F.col("_failed_checks")) == 0, F.lit("PASSED")).otherwise(F.lit("FAILED")),
        )
        .withColumn(
            "quality_status",
            F.when(F.size(F.col("_failed_checks")) == 0, F.lit("PASS")).otherwise(F.lit("FAIL")),
        )
        .withColumn("_validation_timestamp", F.lit(validation_ts).cast(TimestampType()))
        .drop("_failed_checks")
    )


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.col("quality_status") == "PASS")
    rejected = df.filter(F.col("quality_status") == "FAIL")
    return valid, rejected


def write_silver_table(spark: SparkSession, df: DataFrame, table_fqn: str) -> int:
    df.write.format("delta").mode(SILVER_WRITE_MODE).option("overwriteSchema", "true").saveAsTable(
        table_fqn
    )
    return spark.table(table_fqn).count()


def metrics_to_dataframe(spark: SparkSession, metrics: list[CheckMetric]) -> DataFrame:
    rows = [
        (
            m.dataset,
            m.check,
            m.total,
            m.passed,
            m.failed,
            m.pass_pct,
            m.fail_pct,
            m.validation_timestamp,
        )
        for m in metrics
    ]
    return spark.createDataFrame(rows, schema=REPORT_SCHEMA)


def write_quality_report(spark: SparkSession, metrics: list[CheckMetric], validation_ts: datetime) -> int:
    report_df = metrics_to_dataframe(spark, metrics)
    report_df.write.format("delta").mode(SILVER_WRITE_MODE).option("overwriteSchema", "true").saveAsTable(
        DATA_QUALITY_REPORT_TABLE
    )
    return spark.table(DATA_QUALITY_REPORT_TABLE).count()


def collect_metrics(
    df: DataFrame,
    dataset: str,
    check_codes: list[str],
    validation_ts: datetime,
) -> list[CheckMetric]:
    return [metric_for_check(df, dataset, code, validation_ts) for code in check_codes]


def mark_duplicate_keys(df: DataFrame, key_col: str, check_code: str) -> DataFrame:
    """Flag every row whose key appears more than once."""
    window = Window.partitionBy(key_col)
    return add_check_failure(
        df,
        F.count(F.lit(1)).over(window) > 1,
        check_code,
    )


def run_check_stage(
    df: DataFrame,
    dataset: str,
    check_codes: list[str],
    applier: Callable[[DataFrame], DataFrame],
    validation_ts: datetime,
) -> tuple[DataFrame, list[CheckMetric]]:
    df = init_failed_checks(applier(df))
    metrics = collect_metrics(df, dataset, check_codes, validation_ts)
    return df, metrics


def new_batch_id() -> str:
    return os.environ.get("SILVER_BATCH_ID", str(uuid.uuid4()))


def print_silver_summary(
    metrics: list[CheckMetric],
    counts: dict[str, dict[str, int]],
    validation_ts: datetime,
) -> None:
    print("=" * 50)
    print("SILVER VALIDATION SUMMARY")
    print("=" * 50)
    for entity, c in counts.items():
        print(f"\n{entity.title()}:")
        print(f"  PASS (silver):   {c['valid']:,}")
        print(f"  FAIL (rejected): {c['rejected']:,}")
        print(f"  Total evaluated: {c['total']:,}")
    print("\nData quality checks:")
    for m in metrics:
        print(
            f"  [{m.dataset}] {m.check}: total={m.total:,} passed={m.passed:,} "
            f"failed={m.failed:,} pass_pct={m.pass_pct}%"
        )
    print(f"\nValidation timestamp: {validation_ts.isoformat()}Z")
    print("=" * 50)

# --- Completeness checks ---

om __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CUSTOMER_CHECKS = ["CUSTOMER_ID_NULL", "CUSTOMER_EMAIL_NULL"]
ORDER_CHECKS = ["ORDER_CUSTOMER_ID_NULL", "ORDER_PRODUCT_ID_NULL"]
PRODUCT_CHECKS = ["PRODUCT_ID_NULL"]


def apply_customer_completeness(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("customer_id").isNull(), "CUSTOMER_ID_NULL")
    df = add_check_failure(df, F.col("email").isNull(), "CUSTOMER_EMAIL_NULL")
    return df


def apply_order_completeness(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("customer_id").isNull(), "ORDER_CUSTOMER_ID_NULL")
    df = add_check_failure(df, F.col("product_id").isNull(), "ORDER_PRODUCT_ID_NULL")
    return df


def apply_product_completeness(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("product_id").isNull(), "PRODUCT_ID_NULL")
    return df


def run_customer_completeness(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_customer_completeness(df)
    return df, collect_metrics(df, "customers", CUSTOMER_CHECKS, validation_ts)


def run_order_completeness(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_order_completeness(df)
    return df, collect_metrics(df, "orders", ORDER_CHECKS, validation_ts)


def run_product_completeness(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_product_completeness(df)
    return df, collect_metrics(df, "products", PRODUCT_CHECKS, validation_ts)


if __name__ == "__main__":
    # uses shared utilities defined above

    configure_logging()
    spark = get_spark("silver-completeness")
    ts = datetime.utcnow()
    df, metrics = run_customer_completeness(read_bronze_table(spark, BRONZE_CUSTOMERS_TABLE), ts)
    for m in metrics:
        print(f"{m.check}: failed={m.failed}")
