#!/usr/bin/env python3
"""
Silver orchestration: Bronze → Validate → Flag → PASS/Silver + FAIL/Rejected → Quality Report.

Processing order: customers → products → orders (FK checks use valid Silver parents).
Bronze tables are read-only and never modified.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import importlib

completeness = importlib.import_module("01_quality_completeness")
uniqueness = importlib.import_module("02_quality_uniqueness")
type_validation = importlib.import_module("03_quality_type_validation")
referential_integrity = importlib.import_module("04_quality_referential_integrity")
business_logic = importlib.import_module("05_quality_business_logic")

_silver = importlib.import_module("01_quality_completeness")
BRONZE_CUSTOMERS_TABLE = _silver.BRONZE_CUSTOMERS_TABLE
BRONZE_ORDERS_TABLE = _silver.BRONZE_ORDERS_TABLE
BRONZE_PRODUCTS_TABLE = _silver.BRONZE_PRODUCTS_TABLE
DATA_QUALITY_REPORT_TABLE = _silver.DATA_QUALITY_REPORT_TABLE
EXPECTED_DUP_CUSTOMER_KEYS = _silver.EXPECTED_DUP_CUSTOMER_KEYS
EXPECTED_DUP_ORDER_KEYS = _silver.EXPECTED_DUP_ORDER_KEYS
EXPECTED_INVALID_ORDER_CUSTOMER = _silver.EXPECTED_INVALID_ORDER_CUSTOMER
EXPECTED_INVALID_ORDER_PRODUCT = _silver.EXPECTED_INVALID_ORDER_PRODUCT
EXPECTED_NULL_EMAILS = _silver.EXPECTED_NULL_EMAILS
EXPECTED_NULL_ORDER_CUSTOMER = _silver.EXPECTED_NULL_ORDER_CUSTOMER
EXPECTED_NULL_ORDER_PRODUCT = _silver.EXPECTED_NULL_ORDER_PRODUCT
SILVER_CUSTOMERS_REJECTED_TABLE = _silver.SILVER_CUSTOMERS_REJECTED_TABLE
SILVER_CUSTOMERS_TABLE = _silver.SILVER_CUSTOMERS_TABLE
SILVER_ORDERS_REJECTED_TABLE = _silver.SILVER_ORDERS_REJECTED_TABLE
SILVER_ORDERS_TABLE = _silver.SILVER_ORDERS_TABLE
SILVER_PRODUCTS_REJECTED_TABLE = _silver.SILVER_PRODUCTS_REJECTED_TABLE
SILVER_PRODUCTS_TABLE = _silver.SILVER_PRODUCTS_TABLE
CheckMetric = _silver.CheckMetric
configure_logging = _silver.configure_logging
ensure_silver_database = _silver.ensure_silver_database
finalize_quality_columns = _silver.finalize_quality_columns
get_spark = _silver.get_spark
init_failed_checks = _silver.init_failed_checks
LOGGER = _silver.LOGGER
print_silver_summary = _silver.print_silver_summary
read_bronze_table = _silver.read_bronze_table
split_valid_rejected = _silver.split_valid_rejected
write_quality_report = _silver.write_quality_report
write_silver_table = _silver.write_silver_table
from pyspark.sql import functions as F  # noqa: E402


def validate_customers_pipeline(df, validation_ts) -> tuple:
    metrics: list[CheckMetric] = []
    df = init_failed_checks(df)
    df, m = completeness.run_customer_completeness(df, validation_ts)
    metrics.extend(m)
    df, m = uniqueness.run_customer_uniqueness(df, validation_ts)
    metrics.extend(m)
    df, m = type_validation.run_customer_type_validation(df, validation_ts)
    metrics.extend(m)
    df, m = business_logic.run_customer_business_logic(df, validation_ts)
    metrics.extend(m)
    df = finalize_quality_columns(df, validation_ts)
    return df, metrics


def validate_products_pipeline(df, validation_ts) -> tuple:
    metrics: list[CheckMetric] = []
    df = init_failed_checks(df)
    df, m = completeness.run_product_completeness(df, validation_ts)
    metrics.extend(m)
    df, m = uniqueness.run_product_uniqueness(df, validation_ts)
    metrics.extend(m)
    df, m = type_validation.run_product_type_validation(df, validation_ts)
    metrics.extend(m)
    df, m = business_logic.run_product_business_logic(df, validation_ts)
    metrics.extend(m)
    df = finalize_quality_columns(df, validation_ts)
    return df, metrics


def validate_orders_pipeline(df, valid_customers, valid_products, validation_ts) -> tuple:
    metrics: list[CheckMetric] = []
    df = init_failed_checks(df)
    df, m = completeness.run_order_completeness(df, validation_ts)
    metrics.extend(m)
    df, m = uniqueness.run_order_uniqueness(df, validation_ts)
    metrics.extend(m)
    df, m = type_validation.run_order_type_validation(df, validation_ts)
    metrics.extend(m)
    df, m = referential_integrity.run_order_referential_integrity(
        df, valid_customers, valid_products, validation_ts
    )
    metrics.extend(m)
    df, m = business_logic.run_order_business_logic(df, validation_ts)
    metrics.extend(m)
    df = finalize_quality_columns(df, validation_ts)
    return df, metrics


def add_overall_metrics(
    metrics: list[CheckMetric],
    dataset: str,
    total: int,
    rejected: int,
    validation_ts: datetime,
) -> None:
    passed = total - rejected
    metrics.append(CheckMetric(dataset, "OVERALL_ROW_VALIDITY", total, passed, rejected, validation_ts))


def verify_intentional_issues(spark, counts: dict[str, dict[str, int]]) -> list[str]:
    """Verify sample-data defects are detected in rejected tables (not silently removed)."""
    errors: list[str] = []

    rejected_customers = spark.table(SILVER_CUSTOMERS_REJECTED_TABLE)
    null_emails = rejected_customers.filter(F.col("email").isNull()).count()
    if null_emails < EXPECTED_NULL_EMAILS:
        errors.append(f"NULL emails in rejected customers {null_emails} < {EXPECTED_NULL_EMAILS}")

    dup_customer_keys = (
        rejected_customers.filter(F.col("quality_check_reason").contains("DUPLICATE_CUSTOMER_ID"))
        .select("customer_id")
        .distinct()
        .count()
    )
    if dup_customer_keys < EXPECTED_DUP_CUSTOMER_KEYS:
        errors.append(
            f"duplicate customer_id keys detected {dup_customer_keys} < {EXPECTED_DUP_CUSTOMER_KEYS}"
        )

    rejected_orders = spark.table(SILVER_ORDERS_REJECTED_TABLE)
    null_cust = rejected_orders.filter(F.col("customer_id").isNull()).count()
    if null_cust < EXPECTED_NULL_ORDER_CUSTOMER:
        errors.append(f"NULL order customer_id in rejected {null_cust} < {EXPECTED_NULL_ORDER_CUSTOMER}")

    null_prod = rejected_orders.filter(F.col("product_id").isNull()).count()
    if null_prod < EXPECTED_NULL_ORDER_PRODUCT:
        errors.append(f"NULL order product_id in rejected {null_prod} < {EXPECTED_NULL_ORDER_PRODUCT}")

    invalid_cust = rejected_orders.filter(
        F.col("quality_check_reason").contains("FK_CUSTOMER_MISSING")
    ).count()
    if invalid_cust < EXPECTED_INVALID_ORDER_CUSTOMER:
        errors.append(f"invalid customer_id detected {invalid_cust} < {EXPECTED_INVALID_ORDER_CUSTOMER}")

    invalid_prod = rejected_orders.filter(
        F.col("quality_check_reason").contains("FK_PRODUCT_MISSING")
    ).count()
    if invalid_prod < EXPECTED_INVALID_ORDER_PRODUCT:
        errors.append(f"invalid product_id detected {invalid_prod} < {EXPECTED_INVALID_ORDER_PRODUCT}")

    dup_order_keys = (
        rejected_orders.filter(F.col("quality_check_reason").contains("DUPLICATE_ORDER_ID"))
        .select("order_id")
        .distinct()
        .count()
    )
    if dup_order_keys < EXPECTED_DUP_ORDER_KEYS:
        errors.append(f"duplicate order_id keys detected {dup_order_keys} < {EXPECTED_DUP_ORDER_KEYS}")

    for entity, c in counts.items():
        if c["valid"] + c["rejected"] != c["total"]:
            errors.append(f"{entity}: valid+rejected != total ({c})")

    return errors


def run_silver_pipeline() -> int:
    configure_logging()
    validation_ts = datetime.utcnow()
    spark = get_spark("silver-create-tables")
    all_metrics: list[CheckMetric] = []

    LOGGER.info("START Silver pipeline — read Bronze (no modifications)")

    try:
        ensure_silver_database(spark)

        bronze_customers = read_bronze_table(spark, BRONZE_CUSTOMERS_TABLE)
        bronze_products = read_bronze_table(spark, BRONZE_PRODUCTS_TABLE)
        bronze_orders = read_bronze_table(spark, BRONZE_ORDERS_TABLE)

        customers_eval, customer_metrics = validate_customers_pipeline(bronze_customers, validation_ts)
        all_metrics.extend(customer_metrics)
        valid_customers, rejected_customers = split_valid_rejected(customers_eval)

        products_eval, product_metrics = validate_products_pipeline(bronze_products, validation_ts)
        all_metrics.extend(product_metrics)
        valid_products, rejected_products = split_valid_rejected(products_eval)

        orders_eval, order_metrics = validate_orders_pipeline(
            bronze_orders, valid_customers, valid_products, validation_ts
        )
        all_metrics.extend(order_metrics)
        valid_orders, rejected_orders = split_valid_rejected(orders_eval)

        counts = {
            "customers": {
                "total": customers_eval.count(),
                "valid": valid_customers.count(),
                "rejected": rejected_customers.count(),
            },
            "products": {
                "total": products_eval.count(),
                "valid": valid_products.count(),
                "rejected": rejected_products.count(),
            },
            "orders": {
                "total": orders_eval.count(),
                "valid": valid_orders.count(),
                "rejected": rejected_orders.count(),
            },
        }

        add_overall_metrics(all_metrics, "customers", counts["customers"]["total"], counts["customers"]["rejected"], validation_ts)
        add_overall_metrics(all_metrics, "products", counts["products"]["total"], counts["products"]["rejected"], validation_ts)
        add_overall_metrics(all_metrics, "orders", counts["orders"]["total"], counts["orders"]["rejected"], validation_ts)

        write_silver_table(spark, valid_customers, SILVER_CUSTOMERS_TABLE)
        write_silver_table(spark, rejected_customers, SILVER_CUSTOMERS_REJECTED_TABLE)
        write_silver_table(spark, valid_products, SILVER_PRODUCTS_TABLE)
        write_silver_table(spark, rejected_products, SILVER_PRODUCTS_REJECTED_TABLE)
        write_silver_table(spark, valid_orders, SILVER_ORDERS_TABLE)
        write_silver_table(spark, rejected_orders, SILVER_ORDERS_REJECTED_TABLE)
        report_rows = write_quality_report(spark, all_metrics, validation_ts)

        verification_errors = verify_intentional_issues(spark, counts)
        if verification_errors:
            raise ValueError("; ".join(verification_errors))

        print_silver_summary(all_metrics, counts, validation_ts)
        LOGGER.info(
            "END Silver pipeline — SUCCESS | report_rows=%s table=%s",
            report_rows,
            DATA_QUALITY_REPORT_TABLE,
        )
        return 0
    except Exception as exc:
        LOGGER.exception("END Silver pipeline — FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(run_silver_pipeline())
