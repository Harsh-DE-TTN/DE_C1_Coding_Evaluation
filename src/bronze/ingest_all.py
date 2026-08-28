#!/usr/bin/env python3
"""
Bronze ingestion utilities and orchestration for the e-commerce Medallion pipeline.

Bronze = RAW. No deduplication, cleansing, FK validation, or business transforms.
Orchestrates customers → orders → products ingestion.
"""


from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration — override via environment variables or Databricks widgets
# ---------------------------------------------------------------------------

# Required: set on Databricks (Volume, DBFS, or cloud storage prefix).
# Examples:
#   /Volumes/<catalog>/<schema>/<volume>/data
#   dbfs:/FileStore/ecommerce/data
#   s3://<bucket>/ecommerce/raw/
INPUT_PATH = os.environ.get("BRONZE_INPUT_PATH")

BRONZE_DATABASE = os.environ.get("BRONZE_DATABASE", "bronze")

CUSTOMERS_FILE = os.environ.get("BRONZE_CUSTOMERS_FILE", "customers.csv")
ORDERS_FILE = os.environ.get("BRONZE_ORDERS_FILE", "orders.csv")
PRODUCTS_FILE = os.environ.get("BRONZE_PRODUCTS_FILE", "products.csv")

# overwrite = full reload of bronze table per run (dev-friendly)
# append    = retain prior bronze batches (production-style)
BRONZE_WRITE_MODE = os.environ.get("BRONZE_WRITE_MODE", "overwrite")

# Optional external table location (DBFS/S3/Volumes). None = managed table default.
BRONZE_TABLE_LOCATION = os.environ.get("BRONZE_TABLE_LOCATION")

# DECIMAL precision/scale for money columns
MONEY_TYPE = DecimalType(12, 2)
UNIT_PRICE_TYPE = DecimalType(10, 2)

# Expected row counts (from generated sample data)
EXPECTED_CUSTOMER_ROWS = 10_000
EXPECTED_ORDER_ROWS = 100_000
EXPECTED_PRODUCT_ROWS = 500

# Minimum preserved intentional defects (from data generation spec)
MIN_NULL_EMAILS = 50
MIN_DUP_CUSTOMER_IDS = 10
MIN_NULL_ORDER_CUSTOMER = 100
MIN_NULL_ORDER_PRODUCT = 200
MIN_INVALID_ORDER_CUSTOMER = 50
MIN_INVALID_ORDER_PRODUCT = 30
MIN_DUP_ORDER_IDS = 20

VALID_CUSTOMER_ID_MIN = 1
VALID_CUSTOMER_ID_MAX = 10_000
VALID_PRODUCT_ID_MIN = 1
VALID_PRODUCT_ID_MAX = 500

LOGGER = logging.getLogger("bronze_ingest")

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("customer_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("signup_date", DateType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("lifetime_value", MONEY_TYPE, nullable=True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("product_id", IntegerType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", UNIT_PRICE_TYPE, nullable=True),
        StructField("total_amount", MONEY_TYPE, nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("payment_date", DateType(), nullable=True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=False),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("price", UNIT_PRICE_TYPE, nullable=True),
        StructField("cost", UNIT_PRICE_TYPE, nullable=True),
        StructField("stock_quantity", IntegerType(), nullable=True),
        StructField("reorder_level", IntegerType(), nullable=True),
    ]
)

METADATA_COLUMNS = ("_ingestion_timestamp", "_ingestion_date", "_source_file")

TABLE_CONFIG: dict[str, dict[str, Any]] = {
    "customers": {
        "source_file": CUSTOMERS_FILE,
        "table_name": "bronze_customers",
        "schema": CUSTOMERS_SCHEMA,
        "expected_rows": EXPECTED_CUSTOMER_ROWS,
    },
    "orders": {
        "source_file": ORDERS_FILE,
        "table_name": "bronze_orders",
        "schema": ORDERS_SCHEMA,
        "expected_rows": EXPECTED_ORDER_ROWS,
    },
    "products": {
        "source_file": PRODUCTS_FILE,
        "table_name": "bronze_products",
        "schema": PRODUCTS_SCHEMA,
        "expected_rows": EXPECTED_PRODUCT_ROWS,
    },
}


@dataclass
class IngestResult:
    """Outcome of a single Bronze ingestion."""

    entity: str
    source_file: str
    source_path: str
    table_fqn: str
    rows_read: int
    rows_written: int
    status: str
    ingestion_timestamp: datetime
    validation_errors: list[str]

    @property
    def succeeded(self) -> bool:
        return self.status == "SUCCESS"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_spark(app_name: str = "bronze-ingest") -> SparkSession:
    """Return the active Databricks Spark session (Delta Lake enabled on cluster)."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def resolve_source_path(filename: str) -> str:
    """Build the Spark-readable source CSV path from configured input prefix."""
    if not INPUT_PATH:
        raise ValueError(
            "BRONZE_INPUT_PATH is required. "
            "Example: /Volumes/main/raw/data or dbfs:/FileStore/ecommerce/data"
        )
    return f"{INPUT_PATH.rstrip('/')}/{filename}"


def ensure_bronze_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")


def read_bronze_csv(spark: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    """
    Read CSV with explicit schema. Preserves NULLs and malformed rows as nulls
  in typed columns — no cleansing.
    """
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(source_path)
    )


def add_ingestion_metadata(df: DataFrame, source_path: str) -> DataFrame:
    """Add technical lineage columns only; business columns are untouched."""
    ingestion_ts = datetime.utcnow()
    return (
        df.withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("_ingestion_date", F.lit(ingestion_ts.date()).cast(DateType()))
        .withColumn("_source_file", F.lit(source_path))
    )


def write_bronze_table(spark: SparkSession, df: DataFrame, table_name: str) -> int:
    """Write DataFrame to a Bronze Delta table and return written row count."""
    full_name = f"{BRONZE_DATABASE}.{table_name}"
    writer = (
        df.write.format("delta")
        .mode(BRONZE_WRITE_MODE)
        .option("overwriteSchema", "true")
    )

    if BRONZE_TABLE_LOCATION:
        location = f"{BRONZE_TABLE_LOCATION.rstrip('/')}/{table_name}"
        writer = writer.option("path", location)

    writer.saveAsTable(full_name)
    return spark.table(full_name).count()


def _validate_schema(df: DataFrame, business_schema: StructType) -> list[str]:
    errors: list[str] = []
    expected_cols = [f.name for f in business_schema.fields] + list(METADATA_COLUMNS)
    actual_cols = df.columns
    missing = [c for c in expected_cols if c not in actual_cols]
    if missing:
        errors.append(f"Missing columns: {missing}")
    return errors


def _validate_row_count(entity: str, count: int, expected: int) -> list[str]:
    if count != expected:
        return [f"{entity}: row count {count} != expected {expected}"]
    return []


def validate_customers_raw(df: DataFrame) -> list[str]:
    errors = _validate_schema(df, CUSTOMERS_SCHEMA)
    errors.extend(_validate_row_count("customers", df.count(), EXPECTED_CUSTOMER_ROWS))

    null_emails = df.filter(F.col("email").isNull()).count()
    if null_emails < MIN_NULL_EMAILS:
        errors.append(f"NULL emails {null_emails} < expected minimum {MIN_NULL_EMAILS}")

    dupes = df.groupBy("customer_id").count().filter(F.col("count") > 1).count()
    if dupes < MIN_DUP_CUSTOMER_IDS:
        errors.append(
            f"duplicate customer_id keys {dupes} < expected minimum {MIN_DUP_CUSTOMER_IDS}"
        )
    return errors


def validate_orders_raw(df: DataFrame) -> list[str]:
    errors = _validate_schema(df, ORDERS_SCHEMA)
    errors.extend(_validate_row_count("orders", df.count(), EXPECTED_ORDER_ROWS))

    null_customer = df.filter(F.col("customer_id").isNull()).count()
    if null_customer < MIN_NULL_ORDER_CUSTOMER:
        errors.append(
            f"NULL customer_id {null_customer} < expected minimum {MIN_NULL_ORDER_CUSTOMER}"
        )

    null_product = df.filter(F.col("product_id").isNull()).count()
    if null_product < MIN_NULL_ORDER_PRODUCT:
        errors.append(
            f"NULL product_id {null_product} < expected minimum {MIN_NULL_ORDER_PRODUCT}"
        )

    invalid_customer = df.filter(
        F.col("customer_id").isNotNull()
        & (
            (F.col("customer_id") < VALID_CUSTOMER_ID_MIN)
            | (F.col("customer_id") > VALID_CUSTOMER_ID_MAX)
        )
    ).count()
    if invalid_customer < MIN_INVALID_ORDER_CUSTOMER:
        errors.append(
            f"invalid customer_id {invalid_customer} < expected minimum {MIN_INVALID_ORDER_CUSTOMER}"
        )

    invalid_product = df.filter(
        F.col("product_id").isNotNull()
        & (
            (F.col("product_id") < VALID_PRODUCT_ID_MIN)
            | (F.col("product_id") > VALID_PRODUCT_ID_MAX)
        )
    ).count()
    if invalid_product < MIN_INVALID_ORDER_PRODUCT:
        errors.append(
            f"invalid product_id {invalid_product} < expected minimum {MIN_INVALID_ORDER_PRODUCT}"
        )

    dupes = df.groupBy("order_id").count().filter(F.col("count") > 1).count()
    if dupes < MIN_DUP_ORDER_IDS:
        errors.append(f"duplicate order_id keys {dupes} < expected minimum {MIN_DUP_ORDER_IDS}")
    return errors


def validate_products_raw(df: DataFrame) -> list[str]:
    errors = _validate_schema(df, PRODUCTS_SCHEMA)
    errors.extend(_validate_row_count("products", df.count(), EXPECTED_PRODUCT_ROWS))
    return errors


VALIDATORS = {
    "customers": validate_customers_raw,
    "orders": validate_orders_raw,
    "products": validate_products_raw,
}


def ingest_entity(spark: SparkSession, entity: str) -> IngestResult:
    """Ingest one entity from CSV into its Bronze Delta table."""
    if entity not in TABLE_CONFIG:
        raise ValueError(f"Unknown entity: {entity}")

    config = TABLE_CONFIG[entity]
    source_file = config["source_file"]
    table_name = config["table_name"]
    schema = config["schema"]
    ingestion_ts = datetime.utcnow()

    source_path = resolve_source_path(source_file)
    table_fqn = f"{BRONZE_DATABASE}.{table_name}"

    LOGGER.info("START %s ingestion from %s", entity.upper(), source_path)

    try:
        ensure_bronze_database(spark)

        raw_df = read_bronze_csv(spark, source_path, schema)
        rows_read = raw_df.count()
        if rows_read == 0:
            raise ValueError(f"No rows read from {source_path}")

        bronze_df = add_ingestion_metadata(raw_df, source_path)
        rows_written = write_bronze_table(spark, bronze_df, table_name)

        written_df = spark.table(table_fqn)
        validation_errors = VALIDATORS[entity](written_df)
        if validation_errors:
            raise ValueError("; ".join(validation_errors))

        LOGGER.info(
            "%s → SUCCESS | read=%s written=%s table=%s",
            entity.upper(),
            rows_read,
            rows_written,
            table_fqn,
        )
        return IngestResult(
            entity=entity,
            source_file=source_file,
            source_path=source_path,
            table_fqn=table_fqn,
            rows_read=rows_read,
            rows_written=rows_written,
            status="SUCCESS",
            ingestion_timestamp=ingestion_ts,
            validation_errors=[],
        )
    except Exception as exc:
        LOGGER.exception("%s → FAILED: %s", entity.upper(), exc)
        return IngestResult(
            entity=entity,
            source_file=source_file,
            source_path=source_path,
            table_fqn=table_fqn,
            rows_read=0,
            rows_written=0,
            status="FAILED",
            ingestion_timestamp=ingestion_ts,
            validation_errors=[str(exc)],
        )


def print_ingestion_summary(results: list[IngestResult], ingestion_timestamp: datetime) -> None:
    """Print calculated Bronze ingestion summary."""
    print("=" * 40)
    print("BRONZE INGESTION SUMMARY")
    print("=" * 40)
    print()

    labels = {"customers": "Customers", "orders": "Orders", "products": "Products"}
    for result in results:
        label = labels.get(result.entity, result.entity.title())
        print(f"{label}:")
        print(f"  Source: {result.source_file}")
        print(f"  Rows read: {result.rows_read:,}")
        print(f"  Rows written: {result.rows_written:,}")
        print(f"  Status: {result.status}")
        if result.validation_errors:
            for err in result.validation_errors:
                print(f"  Error: {err}")
        print()

    print(f"Ingestion timestamp: {ingestion_timestamp.isoformat()}Z")
    print("=" * 40)


def run_ingest(entity: str) -> int:
    """Entry helper for single-entity scripts. Returns process exit code."""
    configure_logging()
    spark = get_spark(f"bronze-ingest-{entity}")
    result = ingest_entity(spark, entity)
    print_ingestion_summary([result], result.ingestion_timestamp)
    return 0 if result.succeeded else 1


def run_ingest_all() -> int:
    """Orchestrate customers → orders → products ingestion."""
    configure_logging()
    LOGGER.info("START Bronze ingest_all")
    spark = get_spark("bronze-ingest-all")

    results: list[IngestResult] = []
    ingestion_ts = datetime.utcnow()

    for entity in ("customers", "orders", "products"):
        result = ingest_entity(spark, entity)
        results.append(result)
        if not result.succeeded:
            LOGGER.error("Stopping pipeline after %s failure", entity.upper())
            print_ingestion_summary(results, ingestion_ts)
            LOGGER.info("END Bronze ingest_all — FAILED")
            return 1

    print_ingestion_summary(results, ingestion_ts)
    LOGGER.info("END Bronze ingest_all — SUCCESS")
    return 0


if __name__ == "__main__":
    print(
        "ingest_all.py provides shared Bronze utilities only.\n"
        "Run separate ingest scripts instead:\n"
        "  python src/bronze/01_ingest_customers.py\n"
        "  python src/bronze/02_ingest_orders.py\n"
        "  python src/bronze/03_ingest_products.py\n"
        "Or run the full ETL: python src/run_full_etl_pipeline.py",
        file=sys.stderr,
    )
    sys.exit(1)