"""Uniqueness checks — duplicate primary keys."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame

import importlib

_silver = importlib.import_module("01_quality_completeness")
CheckMetric = _silver.CheckMetric
collect_metrics = _silver.collect_metrics
mark_duplicate_keys = _silver.mark_duplicate_keys

CUSTOMER_CHECKS = ["DUPLICATE_CUSTOMER_ID"]
ORDER_CHECKS = ["DUPLICATE_ORDER_ID"]
PRODUCT_CHECKS = ["DUPLICATE_PRODUCT_ID"]


def apply_customer_uniqueness(df: DataFrame) -> DataFrame:
    return mark_duplicate_keys(df, "customer_id", "DUPLICATE_CUSTOMER_ID")


def apply_order_uniqueness(df: DataFrame) -> DataFrame:
    return mark_duplicate_keys(df, "order_id", "DUPLICATE_ORDER_ID")


def apply_product_uniqueness(df: DataFrame) -> DataFrame:
    return mark_duplicate_keys(df, "product_id", "DUPLICATE_PRODUCT_ID")


def run_customer_uniqueness(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_customer_uniqueness(df)
    return df, collect_metrics(df, "customers", CUSTOMER_CHECKS, validation_ts)


def run_order_uniqueness(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_order_uniqueness(df)
    return df, collect_metrics(df, "orders", ORDER_CHECKS, validation_ts)


def run_product_uniqueness(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_product_uniqueness(df)
    return df, collect_metrics(df, "products", PRODUCT_CHECKS, validation_ts)


if __name__ == "__main__":
    configure_logging = _silver.configure_logging
    get_spark = _silver.get_spark
    read_bronze_table = _silver.read_bronze_table
    BRONZE_ORDERS_TABLE = _silver.BRONZE_ORDERS_TABLE

    configure_logging()
    spark = get_spark("silver-uniqueness")
    ts = datetime.utcnow()
    df, metrics = run_order_uniqueness(read_bronze_table(spark, BRONZE_ORDERS_TABLE), ts)
    for m in metrics:
        print(f"{m.check}: failed={m.failed}")
