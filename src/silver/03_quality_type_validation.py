"""Type and format validation — expected schemas, domains, and date fields."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import importlib

_silver = importlib.import_module("01_quality_completeness")
VALID_ORDER_STATUSES = _silver.VALID_ORDER_STATUSES
VALID_SEGMENTS = _silver.VALID_SEGMENTS
CheckMetric = _silver.CheckMetric
add_check_failure = _silver.add_check_failure
collect_metrics = _silver.collect_metrics
init_failed_checks = _silver.init_failed_checks
CUSTOMER_CHECKS = [
    "CUSTOMER_NAME_NULL",
    "CUSTOMER_SEGMENT_INVALID",
    "CUSTOMER_SIGNUP_DATE_NULL",
    "CUSTOMER_LIFETIME_VALUE_NULL",
]
ORDER_CHECKS = [
    "ORDER_DATE_NULL",
    "ORDER_QUANTITY_NULL",
    "ORDER_UNIT_PRICE_NULL",
    "ORDER_TOTAL_AMOUNT_NULL",
    "ORDER_STATUS_INVALID",
]
PRODUCT_CHECKS = [
    "PRODUCT_NAME_NULL",
    "PRODUCT_PRICE_NULL",
    "PRODUCT_COST_NULL",
    "PRODUCT_STOCK_NULL",
    "PRODUCT_REORDER_NULL",
]


def apply_customer_type_validation(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("customer_name").isNull(), "CUSTOMER_NAME_NULL")
    df = add_check_failure(
        df,
        F.col("customer_segment").isNull() | (~F.col("customer_segment").isin(*VALID_SEGMENTS)),
        "CUSTOMER_SEGMENT_INVALID",
    )
    df = add_check_failure(df, F.col("signup_date").isNull(), "CUSTOMER_SIGNUP_DATE_NULL")
    df = add_check_failure(df, F.col("lifetime_value").isNull(), "CUSTOMER_LIFETIME_VALUE_NULL")
    return df


def apply_order_type_validation(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("order_date").isNull(), "ORDER_DATE_NULL")
    df = add_check_failure(df, F.col("quantity").isNull(), "ORDER_QUANTITY_NULL")
    df = add_check_failure(df, F.col("unit_price").isNull(), "ORDER_UNIT_PRICE_NULL")
    df = add_check_failure(df, F.col("total_amount").isNull(), "ORDER_TOTAL_AMOUNT_NULL")
    df = add_check_failure(
        df,
        F.col("order_status").isNull() | (~F.col("order_status").isin(*VALID_ORDER_STATUSES)),
        "ORDER_STATUS_INVALID",
    )
    return df


def apply_product_type_validation(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("product_name").isNull(), "PRODUCT_NAME_NULL")
    df = add_check_failure(df, F.col("price").isNull(), "PRODUCT_PRICE_NULL")
    df = add_check_failure(df, F.col("cost").isNull(), "PRODUCT_COST_NULL")
    df = add_check_failure(df, F.col("stock_quantity").isNull(), "PRODUCT_STOCK_NULL")
    df = add_check_failure(df, F.col("reorder_level").isNull(), "PRODUCT_REORDER_NULL")
    return df


def run_customer_type_validation(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_customer_type_validation(df)
    return df, collect_metrics(df, "customers", CUSTOMER_CHECKS, validation_ts)


def run_order_type_validation(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_order_type_validation(df)
    return df, collect_metrics(df, "orders", ORDER_CHECKS, validation_ts)


def run_product_type_validation(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_product_type_validation(df)
    return df, collect_metrics(df, "products", PRODUCT_CHECKS, validation_ts)


if __name__ == "__main__":
    configure_logging = _silver.configure_logging
    get_spark = _silver.get_spark
    read_bronze_table = _silver.read_bronze_table
    BRONZE_PRODUCTS_TABLE = _silver.BRONZE_PRODUCTS_TABLE

    configure_logging()
    spark = get_spark("silver-type-validation")
    ts = datetime.utcnow()
    df, metrics = run_product_type_validation(read_bronze_table(spark, BRONZE_PRODUCTS_TABLE), ts)
    for m in metrics:
        print(f"{m.check}: failed={m.failed}")
