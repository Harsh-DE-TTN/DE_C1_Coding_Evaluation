"""Business logic validation — numeric rules and payment-date relationships."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import importlib

_silver = importlib.import_module("01_quality_completeness")
AMOUNT_TOLERANCE = _silver.AMOUNT_TOLERANCE
CheckMetric = _silver.CheckMetric
add_check_failure = _silver.add_check_failure
collect_metrics = _silver.collect_metrics
init_failed_checks = _silver.init_failed_checks

CUSTOMER_CHECKS: list[str] = []
ORDER_CHECKS = [
    "ORDER_QUANTITY_NOT_POSITIVE",
    "ORDER_UNIT_PRICE_NOT_POSITIVE",
    "ORDER_TOTAL_AMOUNT_NOT_POSITIVE",
    "ORDER_AMOUNT_MISMATCH",
    "PAYMENT_DATE_MISSING_FOR_COMPLETED",
    "PAYMENT_DATE_UNEXPECTED_FOR_NON_COMPLETED",
]
PRODUCT_CHECKS = [
    "PRODUCT_PRICE_NOT_POSITIVE",
    "PRODUCT_COST_NOT_POSITIVE",
    "PRODUCT_COST_NOT_LESS_THAN_PRICE",
    "PRODUCT_STOCK_NEGATIVE",
    "PRODUCT_REORDER_NEGATIVE",
]


def apply_customer_business_logic(df: DataFrame) -> DataFrame:
    return init_failed_checks(df)


def apply_product_business_logic(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("price").isNull() | (F.col("price") <= 0), "PRODUCT_PRICE_NOT_POSITIVE")
    df = add_check_failure(df, F.col("cost").isNull() | (F.col("cost") <= 0), "PRODUCT_COST_NOT_POSITIVE")
    df = add_check_failure(
        df,
        F.col("price").isNotNull()
        & F.col("cost").isNotNull()
        & (F.col("cost") >= F.col("price")),
        "PRODUCT_COST_NOT_LESS_THAN_PRICE",
    )
    df = add_check_failure(df, F.col("stock_quantity").isNull() | (F.col("stock_quantity") < 0), "PRODUCT_STOCK_NEGATIVE")
    df = add_check_failure(
        df, F.col("reorder_level").isNull() | (F.col("reorder_level") < 0), "PRODUCT_REORDER_NEGATIVE"
    )
    return df


def apply_order_business_logic(df: DataFrame) -> DataFrame:
    df = init_failed_checks(df)
    df = add_check_failure(df, F.col("quantity").isNull() | (F.col("quantity") <= 0), "ORDER_QUANTITY_NOT_POSITIVE")
    df = add_check_failure(
        df, F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "ORDER_UNIT_PRICE_NOT_POSITIVE"
    )
    df = add_check_failure(
        df,
        F.col("total_amount").isNull() | (F.col("total_amount") <= 0),
        "ORDER_TOTAL_AMOUNT_NOT_POSITIVE",
    )
    expected_total = F.col("quantity") * F.col("unit_price")
    df = add_check_failure(
        df,
        F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & F.col("total_amount").isNotNull()
        & (F.abs(F.col("total_amount") - expected_total) > AMOUNT_TOLERANCE),
        "ORDER_AMOUNT_MISMATCH",
    )
    df = add_check_failure(
        df,
        (F.col("order_status") == "Completed") & F.col("payment_date").isNull(),
        "PAYMENT_DATE_MISSING_FOR_COMPLETED",
    )
    df = add_check_failure(
        df,
        F.col("order_status").isin("Pending", "Cancelled") & F.col("payment_date").isNotNull(),
        "PAYMENT_DATE_UNEXPECTED_FOR_NON_COMPLETED",
    )
    return df


def run_customer_business_logic(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_customer_business_logic(df)
    return df, collect_metrics(df, "customers", CUSTOMER_CHECKS, validation_ts)


def run_order_business_logic(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_order_business_logic(df)
    return df, collect_metrics(df, "orders", ORDER_CHECKS, validation_ts)


def run_product_business_logic(df: DataFrame, validation_ts: datetime) -> tuple[DataFrame, list[CheckMetric]]:
    df = apply_product_business_logic(df)
    return df, collect_metrics(df, "products", PRODUCT_CHECKS, validation_ts)


if __name__ == "__main__":
    configure_logging = _silver.configure_logging
    get_spark = _silver.get_spark
    read_bronze_table = _silver.read_bronze_table
    BRONZE_ORDERS_TABLE = _silver.BRONZE_ORDERS_TABLE

    configure_logging()
    spark = get_spark("silver-business-logic")
    ts = datetime.utcnow()
    df, metrics = run_order_business_logic(read_bronze_table(spark, BRONZE_ORDERS_TABLE), ts)
    for m in metrics:
        print(f"{m.check}: failed={m.failed}")
