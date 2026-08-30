#!/usr/bin/env python3
"""
Bronze ingestion utilities and orchestration for the e-commerce Medallion pipeline.

Bronze = RAW.
No deduplication, cleansing, FK validation, or business transformations.

Orchestrates:
    customers → orders → products
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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


# ============================================================================
# Configuration
# ============================================================================

BRONZE_DATABASE = os.environ.get(
    "BRONZE_DATABASE",
    "bronze",
)

CUSTOMERS_FILE = os.environ.get(
    "BRONZE_CUSTOMERS_FILE",
    "customers.csv",
)

ORDERS_FILE = os.environ.get(
    "BRONZE_ORDERS_FILE",
    "orders.csv",
)

PRODUCTS_FILE = os.environ.get(
    "BRONZE_PRODUCTS_FILE",
    "products.csv",
)

BRONZE_WRITE_MODE = os.environ.get(
    "BRONZE_WRITE_MODE",
    "overwrite",
)

BRONZE_TABLE_LOCATION = os.environ.get(
    "BRONZE_TABLE_LOCATION"
)

MONEY_TYPE = DecimalType(12, 2)
UNIT_PRICE_TYPE = DecimalType(10, 2)


# ============================================================================
# Expected data quality
# ============================================================================

EXPECTED_CUSTOMER_ROWS = 10_000
EXPECTED_ORDER_ROWS = 100_000
EXPECTED_PRODUCT_ROWS = 500

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


# ============================================================================
# Schemas
# ============================================================================

CUSTOMERS_SCHEMA = StructType(
    [
        StructField(
            "customer_id",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "customer_name",
            StringType(),
            nullable=True,
        ),
        StructField(
            "email",
            StringType(),
            nullable=True,
        ),
        StructField(
            "country",
            StringType(),
            nullable=True,
        ),
        StructField(
            "signup_date",
            DateType(),
            nullable=True,
        ),
        StructField(
            "customer_segment",
            StringType(),
            nullable=True,
        ),
        StructField(
            "lifetime_value",
            MONEY_TYPE,
            nullable=True,
        ),
    ]
)


ORDERS_SCHEMA = StructType(
    [
        StructField(
            "order_id",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "customer_id",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "order_date",
            DateType(),
            nullable=True,
        ),
        StructField(
            "product_id",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "quantity",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "unit_price",
            UNIT_PRICE_TYPE,
            nullable=True,
        ),
        StructField(
            "total_amount",
            MONEY_TYPE,
            nullable=True,
        ),
        StructField(
            "order_status",
            StringType(),
            nullable=True,
        ),
        StructField(
            "payment_date",
            DateType(),
            nullable=True,
        ),
    ]
)


PRODUCTS_SCHEMA = StructType(
    [
        StructField(
            "product_id",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "product_name",
            StringType(),
            nullable=True,
        ),
        StructField(
            "category",
            StringType(),
            nullable=True,
        ),
        StructField(
            "price",
            UNIT_PRICE_TYPE,
            nullable=True,
        ),
        StructField(
            "cost",
            UNIT_PRICE_TYPE,
            nullable=True,
        ),
        StructField(
            "stock_quantity",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "reorder_level",
            IntegerType(),
            nullable=True,
        ),
    ]
)


METADATA_COLUMNS = (
    "_ingestion_timestamp",
    "_ingestion_date",
    "_source_file",
)


# ============================================================================
# Table configuration
# ============================================================================

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


# ============================================================================
# Result object
# ============================================================================

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


# ============================================================================
# Logging
# ============================================================================

def configure_logging() -> None:
    """Configure application logging."""

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ============================================================================
# Spark
# ============================================================================

def get_spark(
    app_name: str = "bronze-ingest",
) -> SparkSession:
    """Return the active Spark session."""

    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


# ============================================================================
# Source path
# ============================================================================

def resolve_source_path(
    filename: str,
) -> str:
    """
    Resolve a Spark-readable source CSV path.

    IMPORTANT:
    BRONZE_INPUT_PATH is read at runtime rather than at module
    import time.

    This fixes the Databricks problem where the full ETL pipeline
    sets BRONZE_INPUT_PATH after this module has already been imported.
    """

    input_path = os.environ.get(
        "BRONZE_INPUT_PATH"
    )

    # ------------------------------------------------------------------------
    # Fallback to ETL_DATA_OUTPUT_PATH
    # ------------------------------------------------------------------------

    if not input_path:

        input_path = os.environ.get(
            "ETL_DATA_OUTPUT_PATH"
        )

    # ------------------------------------------------------------------------
    # Fallback to generated-data directory
    # ------------------------------------------------------------------------

    if not input_path:

        try:

            from generate_sample_data import (
                resolve_output_dir
            )

            input_path = str(
                resolve_output_dir()
            )

        except Exception as exc:

            LOGGER.warning(
                "Could not resolve default data directory: %s",
                exc,
            )

    # ------------------------------------------------------------------------
    # Final validation
    # ------------------------------------------------------------------------

    if not input_path:

        raise ValueError(
            "BRONZE_INPUT_PATH is required. "
            "Example: "
            "/Volumes/main/raw/data or "
            "dbfs:/FileStore/ecommerce/data"
        )

    input_path = input_path.rstrip("/")

    # ------------------------------------------------------------------------
    # Already absolute paths
    # ------------------------------------------------------------------------

    if filename.startswith(
        (
            "/",
            "dbfs:/",
            "s3://",
            "s3a://",
            "abfss://",
            "wasbs://",
        )
    ):

        return filename

    return f"{input_path}/{filename}"


# ============================================================================
# Database
# ============================================================================

def ensure_bronze_database(
    spark: SparkSession,
) -> None:
    """Create Bronze database if it does not exist."""

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS "
        f"{BRONZE_DATABASE}"
    )


# ============================================================================
# CSV reader
# ============================================================================

def read_bronze_csv(
    spark: SparkSession,
    source_path: str,
    schema: StructType,
) -> DataFrame:
    """
    Read CSV using explicit schema.

    Bronze intentionally preserves bad data.
    """

    return (
        spark.read
        .schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(source_path)
    )


# ============================================================================
# Metadata
# ============================================================================

def add_ingestion_metadata(
    df: DataFrame,
    source_path: str,
) -> DataFrame:
    """Add technical lineage columns."""

    ingestion_ts = datetime.utcnow()

    return (
        df
        .withColumn(
            "_ingestion_timestamp",
            F.lit(ingestion_ts).cast(
                TimestampType()
            ),
        )
        .withColumn(
            "_ingestion_date",
            F.lit(
                ingestion_ts.date()
            ).cast(DateType()),
        )
        .withColumn(
            "_source_file",
            F.lit(source_path),
        )
    )


# ============================================================================
# Bronze writer
# ============================================================================

def write_bronze_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
) -> int:
    """Write DataFrame to Bronze Delta table."""

    full_name = (
        f"{BRONZE_DATABASE}.{table_name}"
    )

    writer = (
        df.write
        .format("delta")
        .mode(BRONZE_WRITE_MODE)
        .option(
            "overwriteSchema",
            "true",
        )
    )

    if BRONZE_TABLE_LOCATION:

        location = (
            f"{BRONZE_TABLE_LOCATION.rstrip('/')}"
            f"/{table_name}"
        )

        writer = writer.option(
            "path",
            location,
        )

    writer.saveAsTable(
        full_name
    )

    return (
        spark.table(full_name)
        .count()
    )


# ============================================================================
# Validation helpers
# ============================================================================

def _validate_schema(
    df: DataFrame,
    business_schema: StructType,
) -> list[str]:

    errors: list[str] = []

    expected_columns = (
        [
            field.name
            for field in business_schema.fields
        ]
        + list(METADATA_COLUMNS)
    )

    actual_columns = df.columns

    missing = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]

    if missing:

        errors.append(
            f"Missing columns: {missing}"
        )

    return errors


def _validate_row_count(
    entity: str,
    count: int,
    expected: int,
) -> list[str]:

    if count != expected:

        return [
            f"{entity}: row count "
            f"{count} != expected {expected}"
        ]

    return []


# ============================================================================
# Customer validation
# ============================================================================

def validate_customers_raw(
    df: DataFrame,
) -> list[str]:

    errors = _validate_schema(
        df,
        CUSTOMERS_SCHEMA,
    )

    row_count = df.count()

    errors.extend(
        _validate_row_count(
            "customers",
            row_count,
            EXPECTED_CUSTOMER_ROWS,
        )
    )

    null_emails = (
        df
        .filter(
            F.col("email").isNull()
        )
        .count()
    )

    if null_emails < MIN_NULL_EMAILS:

        errors.append(
            f"NULL emails {null_emails} "
            f"< expected minimum "
            f"{MIN_NULL_EMAILS}"
        )

    duplicate_customer_ids = (
        df
        .groupBy("customer_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_customer_ids < MIN_DUP_CUSTOMER_IDS:

        errors.append(
            "duplicate customer_id keys "
            f"{duplicate_customer_ids} "
            "< expected minimum "
            f"{MIN_DUP_CUSTOMER_IDS}"
        )

    return errors


# ============================================================================
# Order validation
# ============================================================================

def validate_orders_raw(
    df: DataFrame,
) -> list[str]:

    errors = _validate_schema(
        df,
        ORDERS_SCHEMA,
    )

    row_count = df.count()

    errors.extend(
        _validate_row_count(
            "orders",
            row_count,
            EXPECTED_ORDER_ROWS,
        )
    )

    # ------------------------------------------------------------------------
    # NULL customer IDs
    # ------------------------------------------------------------------------

    null_customer = (
        df
        .filter(
            F.col("customer_id").isNull()
        )
        .count()
    )

    if null_customer < MIN_NULL_ORDER_CUSTOMER:

        errors.append(
            f"NULL customer_id "
            f"{null_customer} "
            f"< expected minimum "
            f"{MIN_NULL_ORDER_CUSTOMER}"
        )

    # ------------------------------------------------------------------------
    # NULL product IDs
    # ------------------------------------------------------------------------

    null_product = (
        df
        .filter(
            F.col("product_id").isNull()
        )
        .count()
    )

    if null_product < MIN_NULL_ORDER_PRODUCT:

        errors.append(
            f"NULL product_id "
            f"{null_product} "
            f"< expected minimum "
            f"{MIN_NULL_ORDER_PRODUCT}"
        )

    # ------------------------------------------------------------------------
    # Invalid customer IDs
    # ------------------------------------------------------------------------

    invalid_customer = (
        df
        .filter(
            F.col("customer_id").isNotNull()
            & (
                (
                    F.col("customer_id")
                    < VALID_CUSTOMER_ID_MIN
                )
                |
                (
                    F.col("customer_id")
                    > VALID_CUSTOMER_ID_MAX
                )
            )
        )
        .count()
    )

    if invalid_customer < MIN_INVALID_ORDER_CUSTOMER:

        errors.append(
            f"invalid customer_id "
            f"{invalid_customer} "
            "< expected minimum "
            f"{MIN_INVALID_ORDER_CUSTOMER}"
        )

    # ------------------------------------------------------------------------
    # Invalid product IDs
    # ------------------------------------------------------------------------

    invalid_product = (
        df
        .filter(
            F.col("product_id").isNotNull()
            & (
                (
                    F.col("product_id")
                    < VALID_PRODUCT_ID_MIN
                )
                |
                (
                    F.col("product_id")
                    > VALID_PRODUCT_ID_MAX
                )
            )
        )
        .count()
    )

    if invalid_product < MIN_INVALID_ORDER_PRODUCT:

        errors.append(
            f"invalid product_id "
            f"{invalid_product} "
            "< expected minimum "
            f"{MIN_INVALID_ORDER_PRODUCT}"
        )

    # ------------------------------------------------------------------------
    # Duplicate order IDs
    # ------------------------------------------------------------------------

    duplicate_order_ids = (
        df
        .groupBy("order_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_order_ids < MIN_DUP_ORDER_IDS:

        errors.append(
            f"duplicate order_id keys "
            f"{duplicate_order_ids} "
            "< expected minimum "
            f"{MIN_DUP_ORDER_IDS}"
        )

    return errors


# ============================================================================
# Product validation
# ============================================================================

def validate_products_raw(
    df: DataFrame,
) -> list[str]:

    errors = _validate_schema(
        df,
        PRODUCTS_SCHEMA,
    )

    row_count = df.count()

    errors.extend(
        _validate_row_count(
            "products",
            row_count,
            EXPECTED_PRODUCT_ROWS,
        )
    )

    return errors


# ============================================================================
# Validator registry
# ============================================================================

VALIDATORS = {
    "customers": validate_customers_raw,
    "orders": validate_orders_raw,
    "products": validate_products_raw,
}


# ============================================================================
# Single entity ingestion
# ============================================================================

def ingest_entity(
    spark: SparkSession,
    entity: str,
) -> IngestResult:
    """Ingest one entity from CSV into Bronze Delta."""

    if entity not in TABLE_CONFIG:

        raise ValueError(
            f"Unknown entity: {entity}"
        )

    config = TABLE_CONFIG[entity]

    source_file = config[
        "source_file"
    ]

    table_name = config[
        "table_name"
    ]

    schema = config[
        "schema"
    ]

    ingestion_ts = datetime.utcnow()

    # ------------------------------------------------------------------------
    # IMPORTANT:
    # resolve_source_path() reads BRONZE_INPUT_PATH at runtime.
    # ------------------------------------------------------------------------

    source_path = resolve_source_path(
        source_file
    )

    table_fqn = (
        f"{BRONZE_DATABASE}.{table_name}"
    )

    LOGGER.info(
        "START %s ingestion from %s",
        entity.upper(),
        source_path,
    )

    try:

        # --------------------------------------------------------------------
        # Database
        # --------------------------------------------------------------------

        ensure_bronze_database(
            spark
        )

        # --------------------------------------------------------------------
        # Read raw CSV
        # --------------------------------------------------------------------

        raw_df = read_bronze_csv(
            spark,
            source_path,
            schema,
        )

        rows_read = raw_df.count()

        if rows_read == 0:

            raise ValueError(
                f"No rows read from "
                f"{source_path}"
            )

        # --------------------------------------------------------------------
        # Add metadata
        # --------------------------------------------------------------------

        bronze_df = add_ingestion_metadata(
            raw_df,
            source_path,
        )

        # --------------------------------------------------------------------
        # Write Bronze
        # --------------------------------------------------------------------

        rows_written = write_bronze_table(
            spark,
            bronze_df,
            table_name,
        )

        # --------------------------------------------------------------------
        # Validate Bronze
        # --------------------------------------------------------------------

        written_df = spark.table(
            table_fqn
        )

        validation_errors = VALIDATORS[
            entity
        ](
            written_df
        )

        if validation_errors:

            raise ValueError(
                "; ".join(
                    validation_errors
                )
            )

        # --------------------------------------------------------------------
        # Success
        # --------------------------------------------------------------------

        LOGGER.info(
            "%s → SUCCESS | "
            "read=%s written=%s table=%s",
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

        LOGGER.exception(
            "%s → FAILED: %s",
            entity.upper(),
            exc,
        )

        return IngestResult(
            entity=entity,
            source_file=source_file,
            source_path=source_path,
            table_fqn=table_fqn,
            rows_read=0,
            rows_written=0,
            status="FAILED",
            ingestion_timestamp=ingestion_ts,
            validation_errors=[
                str(exc)
            ],
        )


# ============================================================================
# Summary
# ============================================================================

def print_ingestion_summary(
    results: list[IngestResult],
    ingestion_timestamp: datetime,
) -> None:
    """Print Bronze ingestion summary."""

    print("=" * 40)
    print("BRONZE INGESTION SUMMARY")
    print("=" * 40)
    print()

    labels = {
        "customers": "Customers",
        "orders": "Orders",
        "products": "Products",
    }

    for result in results:

        label = labels.get(
            result.entity,
            result.entity.title(),
        )

        print(
            f"{label}:"
        )

        print(
            f"  Source: "
            f"{result.source_file}"
        )

        print(
            f"  Rows read: "
            f"{result.rows_read:,}"
        )

        print(
            f"  Rows written: "
            f"{result.rows_written:,}"
        )

        print(
            f"  Status: "
            f"{result.status}"
        )

        if result.validation_errors:

            for error in (
                result.validation_errors
            ):

                print(
                    f"  Error: {error}"
                )

        print()

    print(
        "Ingestion timestamp: "
        f"{ingestion_timestamp.isoformat()}Z"
    )

    print(
        "=" * 40
    )


# ============================================================================
# Single entity entry point
# ============================================================================

def run_ingest(
    entity: str,
) -> int:
    """
    Run ingestion for one entity.

    Returns:
        0 = success
        1 = failure
    """

    configure_logging()

    # ------------------------------------------------------------------------
    # IMPORTANT:
    # Read environment at execution time.
    # ------------------------------------------------------------------------

    LOGGER.info(
        "BRONZE_INPUT_PATH=%s",
        os.environ.get(
            "BRONZE_INPUT_PATH"
        ),
    )

    spark = get_spark(
        f"bronze-ingest-{entity}"
    )

    try:

        result = ingest_entity(
            spark,
            entity,
        )

    except Exception as exc:

        print(
            "\n" + "=" * 80
        )

        print(
            f"BRONZE INGESTION FAILED: "
            f"{entity.upper()}"
        )

        print(
            f"ERROR TYPE: "
            f"{type(exc).__name__}"
        )

        print(
            f"ERROR MESSAGE: {exc}"
        )

        print(
            "=" * 80
        )

        import traceback

        traceback.print_exc()

        return 1

    print_ingestion_summary(
        [result],
        result.ingestion_timestamp,
    )

    return (
        0
        if result.succeeded
        else 1
    )


# ============================================================================
# All Bronze entities
# ============================================================================

def run_ingest_all() -> int:
    """
    Orchestrate:
        customers → orders → products
    """

    configure_logging()

    LOGGER.info(
        "START Bronze ingest_all"
    )

    LOGGER.info(
        "BRONZE_INPUT_PATH=%s",
        os.environ.get(
            "BRONZE_INPUT_PATH"
        ),
    )

    spark = get_spark(
        "bronze-ingest-all"
    )

    results: list[IngestResult] = []

    ingestion_ts = datetime.utcnow()

    for entity in (
        "customers",
        "orders",
        "products",
    ):

        try:

            result = ingest_entity(
                spark,
                entity,
            )

        except Exception as exc:

            print(
                "\n" + "=" * 80
            )

            print(
                f"BRONZE INGESTION FAILED: "
                f"{entity.upper()}"
            )

            print(
                f"ERROR TYPE: "
                f"{type(exc).__name__}"
            )

            print(
                f"ERROR MESSAGE: {exc}"
            )

            print(
                "=" * 80
            )

            import traceback

            traceback.print_exc()

            return 1

        results.append(
            result
        )

        if not result.succeeded:

            LOGGER.error(
                "Stopping pipeline after "
                "%s failure",
                entity.upper(),
            )

            print_ingestion_summary(
                results,
                ingestion_ts,
            )

            LOGGER.info(
                "END Bronze ingest_all — FAILED"
            )

            return 1

    print_ingestion_summary(
        results,
        ingestion_ts,
    )

    LOGGER.info(
        "END Bronze ingest_all — SUCCESS"
    )

    return 0


# ============================================================================
# Script entry point
# ============================================================================

if __name__ == "__main__":

    exit_code = run_ingest_all()

    sys.exit(
        exit_code
    )