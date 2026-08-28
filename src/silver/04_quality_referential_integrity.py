"""Referential integrity — order foreign keys must exist in valid Silver parents."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import importlib

_silver = importlib.import_module("01_quality_completeness")
CheckMetric = _silver.CheckMetric
add_check_failure = _silver.add_check_failure
collect_metrics = _silver.collect_metrics
init_failed_checks = _silver.init_failed_checks

ORDER_CHECKS = ["FK_CUSTOMER_MISSING", "FK_PRODUCT_MISSING"]


def apply_order_referential_integrity(
    orders_df: DataFrame,
    valid_customers_df: DataFrame,
    valid_products_df: DataFrame,
) -> DataFrame:
    df = init_failed_checks(orders_df)

    valid_customer_ids = valid_customers_df.select(
        F.col("customer_id").alias("_valid_customer_id")
    ).distinct()
    valid_product_ids = valid_products_df.select(
        F.col("product_id").alias("_valid_product_id")
    ).distinct()

    df = df.join(valid_customer_ids, df.customer_id == F.col("_valid_customer_id"), "left")
    df = add_check_failure(
        df,
        F.col("customer_id").isNotNull() & F.col("_valid_customer_id").isNull(),
        "FK_CUSTOMER_MISSING",
    ).drop("_valid_customer_id")

    df = df.join(valid_product_ids, df.product_id == F.col("_valid_product_id"), "left")
    df = add_check_failure(
        df,
        F.col("product_id").isNotNull() & F.col("_valid_product_id").isNull(),
        "FK_PRODUCT_MISSING",
    ).drop("_valid_product_id")

    return df


def run_order_referential_integrity(
    orders_df: DataFrame,
    valid_customers_df: DataFrame,
    valid_products_df: DataFrame,
    validation_ts: datetime,
) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_order_referential_integrity(orders_df, valid_customers_df, valid_products_df)
    return df, collect_metrics(df, "orders", ORDER_CHECKS, validation_ts)


if __name__ == "__main__":
    print("Referential integrity runs as part of create_silver_tables.py after customers/products PASS.")
