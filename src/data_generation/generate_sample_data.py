#!/usr/bin/env python3
"""
Generate realistic e-commerce sample CSVs with intentional data-quality issues.

Outputs: data/customers.csv, data/products.csv, data/orders.csv
"""

from __future__ import annotations

import random
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Configuration (centralized)
# ---------------------------------------------------------------------------

CUSTOMER_COUNT = 10_000
ORDER_COUNT = 100_000
PRODUCT_COUNT = 500
RANDOM_SEED = 42

# Intentional quality-issue injection counts (explicit assignment requirements)
NULL_EMAIL_COUNT = 50
DUPLICATE_CUSTOMER_ID_COUNT = 10
NULL_ORDER_CUSTOMER_ID_COUNT = 100
NULL_ORDER_PRODUCT_ID_COUNT = 200
INVALID_ORDER_CUSTOMER_ID_COUNT = 50
INVALID_ORDER_PRODUCT_ID_COUNT = 30
DUPLICATE_ORDER_ID_COUNT = 20

# Invalid FK pools — IDs outside valid customer/product ranges
INVALID_CUSTOMER_ID_START = 90_001
INVALID_PRODUCT_ID_START = 9_001

SEGMENTS = ("Premium", "Standard", "Basic")
SEGMENT_WEIGHTS = (0.15, 0.35, 0.50)
ORDER_STATUSES = ("Pending", "Completed", "Cancelled")
ORDER_STATUS_WEIGHTS = (0.10, 0.82, 0.08)

PRODUCT_CATEGORIES = (
    "Electronics",
    "Clothing",
    "Home & Kitchen",
    "Sports & Outdoors",
    "Books",
    "Beauty",
    "Toys",
    "Grocery",
    "Automotive",
    "Office Supplies",
)

# Repo paths
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
]

PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "stock_quantity",
    "reorder_level",
]

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_status",
    "payment_date",
]


@dataclass(frozen=True)
class IssuePlan:
    """Tracks which row indices receive each injected issue."""

    null_email_indices: list[int]
    duplicate_customer_id_indices: list[int]
    duplicate_customer_id_targets: list[int]
    null_order_customer_indices: list[int]
    null_order_product_indices: list[int]
    invalid_order_customer_indices: list[int]
    invalid_order_product_indices: list[int]
    duplicate_order_id_indices: list[int]
    duplicate_order_id_sources: list[int]


def _money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _setup_random(seed: int) -> tuple[random.Random, Faker]:
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)
    fake.seed_instance(seed)
    return rng, fake


def _weighted_customer_order_counts(
    rng: random.Random, customer_ids: list[int], total_orders: int
) -> dict[int, int]:
    """
    Assign order counts so some customers are heavy buyers and some get zero orders.
    Uses a Pareto-like weighting, then scales to exactly total_orders.
    """
    weights = [rng.paretovariate(1.2) for _ in customer_ids]
    total_weight = sum(weights)
    raw_counts = {
        cid: max(0, int(round((w / total_weight) * total_orders * 1.15)))
        for cid, w in zip(customer_ids, weights)
    }

    # Scale down or up to hit exact total
    current = sum(raw_counts.values())
    ids_by_weight = sorted(customer_ids, key=lambda c: raw_counts[c], reverse=True)

    while current > total_orders:
        for cid in ids_by_weight:
            if current <= total_orders:
                break
            if raw_counts[cid] > 0:
                raw_counts[cid] -= 1
                current -= 1

    while current < total_orders:
        cid = rng.choice(ids_by_weight[: max(1, len(ids_by_weight) // 5)])
        raw_counts[cid] += 1
        current += 1

    return raw_counts


def build_issue_plan(rng: random.Random) -> IssuePlan:
    """Select disjoint row indices for each injected issue type."""
    customer_indices = list(range(CUSTOMER_COUNT))
    rng.shuffle(customer_indices)

    null_email_indices = customer_indices[:NULL_EMAIL_COUNT]

    dup_pool = customer_indices[NULL_EMAIL_COUNT : NULL_EMAIL_COUNT + DUPLICATE_CUSTOMER_ID_COUNT * 2]
    duplicate_customer_id_indices = dup_pool[:DUPLICATE_CUSTOMER_ID_COUNT]
    duplicate_customer_id_targets = dup_pool[DUPLICATE_CUSTOMER_ID_COUNT : DUPLICATE_CUSTOMER_ID_COUNT * 2]

    order_indices = list(range(ORDER_COUNT))
    rng.shuffle(order_indices)

    offset = 0
    null_order_customer_indices = order_indices[offset : offset + NULL_ORDER_CUSTOMER_ID_COUNT]
    offset += NULL_ORDER_CUSTOMER_ID_COUNT

    null_order_product_indices = order_indices[offset : offset + NULL_ORDER_PRODUCT_ID_COUNT]
    offset += NULL_ORDER_PRODUCT_ID_COUNT

    invalid_order_customer_indices = order_indices[offset : offset + INVALID_ORDER_CUSTOMER_ID_COUNT]
    offset += INVALID_ORDER_CUSTOMER_ID_COUNT

    invalid_order_product_indices = order_indices[offset : offset + INVALID_ORDER_PRODUCT_ID_COUNT]
    offset += INVALID_ORDER_PRODUCT_ID_COUNT

    dup_order_pool = order_indices[offset : offset + DUPLICATE_ORDER_ID_COUNT * 2]
    duplicate_order_id_indices = dup_order_pool[:DUPLICATE_ORDER_ID_COUNT]
    duplicate_order_id_sources = dup_order_pool[DUPLICATE_ORDER_ID_COUNT : DUPLICATE_ORDER_ID_COUNT * 2]

    return IssuePlan(
        null_email_indices=null_email_indices,
        duplicate_customer_id_indices=duplicate_customer_id_indices,
        duplicate_customer_id_targets=duplicate_customer_id_targets,
        null_order_customer_indices=null_order_customer_indices,
        null_order_product_indices=null_order_product_indices,
        invalid_order_customer_indices=invalid_order_customer_indices,
        invalid_order_product_indices=invalid_order_product_indices,
        duplicate_order_id_indices=duplicate_order_id_indices,
        duplicate_order_id_sources=duplicate_order_id_sources,
    )


def generate_customers(rng: random.Random, fake: Faker) -> pd.DataFrame:
    """Generate base customer records before quality-issue injection."""
    today = date.today()
    signup_start = date(2020, 1, 1)

    rows: list[dict[str, Any]] = []
    for customer_id in range(1, CUSTOMER_COUNT + 1):
        segment = rng.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        signup_date = fake.date_between(start_date=signup_start, end_date=today)

        if segment == "Premium":
            lifetime_value = _money(rng.uniform(2_500, 25_000))
        elif segment == "Standard":
            lifetime_value = _money(rng.uniform(500, 5_000))
        else:
            lifetime_value = _money(rng.uniform(50, 1_500))

        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": fake.name(),
                "email": fake.email(),
                "country": fake.country(),
                "signup_date": signup_date.isoformat(),
                "customer_segment": segment,
                "lifetime_value": f"{lifetime_value:.2f}",
            }
        )

    return pd.DataFrame(rows, columns=CUSTOMER_COLUMNS)


def inject_customer_quality_issues(
    customers: pd.DataFrame, plan: IssuePlan
) -> pd.DataFrame:
    """Inject NULL emails and duplicate customer_id values."""
    df = customers.copy()

    # Intentional issue: NULL email (completeness)
    for idx in plan.null_email_indices:
        df.at[idx, "email"] = pd.NA

    # Intentional issue: duplicate customer_id (uniqueness)
    for row_idx, target_idx in zip(
        plan.duplicate_customer_id_indices, plan.duplicate_customer_id_targets
    ):
        df.at[row_idx, "customer_id"] = int(df.at[target_idx, "customer_id"])

    return df


def generate_products(rng: random.Random, fake: Faker) -> pd.DataFrame:
    """Generate product catalog with normal business-rule-compliant values."""
    rows: list[dict[str, Any]] = []
    for product_id in range(1, PRODUCT_COUNT + 1):
        category = rng.choice(PRODUCT_CATEGORIES)
        price = _money(rng.uniform(5, 500))
        margin = rng.uniform(0.15, 0.55)
        cost = _money(float(price) * (1 - margin))
        if cost >= price:
            cost = _money(float(price) * 0.7)

        stock_quantity = rng.randint(0, 2_000)
        reorder_level = rng.randint(5, max(5, stock_quantity // 4))

        rows.append(
            {
                "product_id": product_id,
                "product_name": f"{fake.word().title()} {fake.word().title()}",
                "category": category,
                "price": f"{price:.2f}",
                "cost": f"{cost:.2f}",
                "stock_quantity": stock_quantity,
                "reorder_level": reorder_level,
            }
        )

    return pd.DataFrame(rows, columns=PRODUCT_COLUMNS)


def generate_orders(
    rng: random.Random,
    fake: Faker,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """Generate realistic orders with varied customer/product distributions."""
    today = date.today()
    customer_ids = customers["customer_id"].astype(int).tolist()
    product_ids = products["product_id"].astype(int).tolist()
    product_price = {
        int(row.product_id): Decimal(row.price) for row in products.itertuples()
    }
    customer_signup = {
        int(row.customer_id): datetime.strptime(row.signup_date, "%Y-%m-%d").date()
        for row in customers.itertuples()
    }

    order_counts = _weighted_customer_order_counts(rng, customer_ids, ORDER_COUNT)

    rows: list[dict[str, Any]] = []
    order_id = 1

    for customer_id, count in order_counts.items():
        signup = customer_signup[customer_id]
        for _ in range(count):
            product_id = rng.choices(
                product_ids,
                weights=[rng.paretovariate(1.5) for _ in product_ids],
                k=1,
            )[0]
            quantity = rng.randint(1, 8)
            unit_price = product_price[product_id]
            if rng.random() < 0.08:
                unit_price = _money(float(unit_price) * rng.uniform(0.85, 1.15))

            total_amount = _money(float(unit_price) * quantity)
            order_date = fake.date_between(start_date=signup, end_date=today)
            order_status = rng.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

            if order_status == "Completed":
                payment_date = order_date + timedelta(days=rng.randint(0, 5))
                if payment_date > today:
                    payment_date = today
                payment_date_str = payment_date.isoformat()
            else:
                payment_date_str = pd.NA

            rows.append(
                {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_date": order_date.isoformat(),
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": f"{unit_price:.2f}",
                    "total_amount": f"{total_amount:.2f}",
                    "order_status": order_status,
                    "payment_date": payment_date_str,
                }
            )
            order_id += 1

    if len(rows) != ORDER_COUNT:
        raise RuntimeError(f"Expected {ORDER_COUNT} orders, generated {len(rows)}")

    return pd.DataFrame(rows, columns=ORDER_COLUMNS)


def inject_order_quality_issues(orders: pd.DataFrame, plan: IssuePlan) -> pd.DataFrame:
    """Inject NULL FKs, invalid FKs, and duplicate order_id values."""
    df = orders.copy()
    valid_customer_ids = set(range(1, CUSTOMER_COUNT + 1))
    valid_product_ids = set(range(1, PRODUCT_COUNT + 1))

    # Intentional issue: NULL customer_id (completeness)
    for idx in plan.null_order_customer_indices:
        df.at[idx, "customer_id"] = pd.NA

    # Intentional issue: NULL product_id (completeness)
    for idx in plan.null_order_product_indices:
        df.at[idx, "product_id"] = pd.NA

    # Intentional issue: invalid customer_id (referential integrity)
    for i, idx in enumerate(plan.invalid_order_customer_indices):
        invalid_id = INVALID_CUSTOMER_ID_START + i
        if invalid_id in valid_customer_ids:
            raise ValueError(f"Invalid customer_id collides with valid id: {invalid_id}")
        df.at[idx, "customer_id"] = invalid_id

    # Intentional issue: invalid product_id (referential integrity)
    for i, idx in enumerate(plan.invalid_order_product_indices):
        invalid_id = INVALID_PRODUCT_ID_START + i
        if invalid_id in valid_product_ids:
            raise ValueError(f"Invalid product_id collides with valid id: {invalid_id}")
        df.at[idx, "product_id"] = invalid_id

    # Intentional issue: duplicate order_id (uniqueness)
    for row_idx, source_idx in zip(
        plan.duplicate_order_id_indices, plan.duplicate_order_id_sources
    ):
        df.at[row_idx, "order_id"] = int(df.at[source_idx, "order_id"])

    return df


def _file_size_kb(path: Path) -> float:
    return path.stat().st_size / 1024


def validate_generated_data(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    plan: IssuePlan,
) -> dict[str, Any]:
    """Validate generated datasets and return computed metrics."""
    errors: list[str] = []

    for path in (DATA_DIR / "customers.csv", DATA_DIR / "products.csv", DATA_DIR / "orders.csv"):
        if not path.exists():
            errors.append(f"Missing output file: {path}")

    for name, df, cols in (
        ("customers", customers, CUSTOMER_COLUMNS),
        ("products", products, PRODUCT_COLUMNS),
        ("orders", orders, ORDER_COLUMNS),
    ):
        missing = [c for c in cols if c not in df.columns]
        if missing:
            errors.append(f"{name} missing columns: {missing}")

    if len(customers) != CUSTOMER_COUNT:
        errors.append(f"customers row count {len(customers)} != {CUSTOMER_COUNT}")
    if len(products) != PRODUCT_COUNT:
        errors.append(f"products row count {len(products)} != {PRODUCT_COUNT}")
    if len(orders) != ORDER_COUNT:
        errors.append(f"orders row count {len(orders)} != {ORDER_COUNT}")

    null_email_count = int(customers["email"].isna().sum())
    if null_email_count != NULL_EMAIL_COUNT:
        errors.append(f"NULL email count {null_email_count} != {NULL_EMAIL_COUNT}")

    dup_customer_count = int(customers["customer_id"].duplicated().sum())
    if dup_customer_count != DUPLICATE_CUSTOMER_ID_COUNT:
        errors.append(
            f"duplicate customer_id rows {dup_customer_count} != {DUPLICATE_CUSTOMER_ID_COUNT}"
        )

    null_order_customer = int(orders["customer_id"].isna().sum())
    null_order_product = int(orders["product_id"].isna().sum())
    if null_order_customer != NULL_ORDER_CUSTOMER_ID_COUNT:
        errors.append(
            f"NULL order customer_id {null_order_customer} != {NULL_ORDER_CUSTOMER_ID_COUNT}"
        )
    if null_order_product != NULL_ORDER_PRODUCT_ID_COUNT:
        errors.append(
            f"NULL order product_id {null_order_product} != {NULL_ORDER_PRODUCT_ID_COUNT}"
        )

    valid_customer_ids = set(range(1, CUSTOMER_COUNT + 1))
    valid_product_ids = set(range(1, PRODUCT_COUNT + 1))

    non_null_customers = orders["customer_id"].dropna().astype(int)
    invalid_customer_fk = int((~non_null_customers.isin(valid_customer_ids)).sum())
    if invalid_customer_fk != INVALID_ORDER_CUSTOMER_ID_COUNT:
        errors.append(
            f"invalid customer_id count {invalid_customer_fk} != {INVALID_ORDER_CUSTOMER_ID_COUNT}"
        )

    non_null_products = orders["product_id"].dropna().astype(int)
    invalid_product_fk = int((~non_null_products.isin(valid_product_ids)).sum())
    if invalid_product_fk != INVALID_ORDER_PRODUCT_ID_COUNT:
        errors.append(
            f"invalid product_id count {invalid_product_fk} != {INVALID_ORDER_PRODUCT_ID_COUNT}"
        )

    dup_order_count = int(orders["order_id"].duplicated().sum())
    if dup_order_count != DUPLICATE_ORDER_ID_COUNT:
        errors.append(f"duplicate order_id rows {dup_order_count} != {DUPLICATE_ORDER_ID_COUNT}")

    # Business rules on rows without injected FK/null issues
    issue_order_rows = (
        set(plan.null_order_customer_indices)
        | set(plan.null_order_product_indices)
        | set(plan.invalid_order_customer_indices)
        | set(plan.invalid_order_product_indices)
        | set(plan.duplicate_order_id_indices)
    )
    clean_rows = orders[~orders.index.isin(issue_order_rows)].copy()

    clean_rows["quantity"] = clean_rows["quantity"].astype(int)
    clean_rows["unit_price"] = clean_rows["unit_price"].astype(float)
    clean_rows["total_amount"] = clean_rows["total_amount"].astype(float)

    if (clean_rows["quantity"] <= 0).any():
        errors.append("clean orders contain non-positive quantity")
    if (clean_rows["unit_price"] <= 0).any():
        errors.append("clean orders contain non-positive unit_price")

    amount_mismatch = (
        (clean_rows["total_amount"] - clean_rows["quantity"] * clean_rows["unit_price"]).abs() > 0.01
    ).sum()
    if amount_mismatch > 0:
        errors.append(f"clean orders with total_amount mismatch: {amount_mismatch}")

    products_df = products.copy()
    products_df["price"] = products_df["price"].astype(float)
    products_df["cost"] = products_df["cost"].astype(float)
    if (products_df["price"] <= 0).any() or (products_df["cost"] <= 0).any():
        errors.append("products contain non-positive price/cost")
    if (products_df["cost"] >= products_df["price"]).any():
        errors.append("products contain cost >= price")

    customers_with_orders = set(orders["customer_id"].dropna().astype(int)) & valid_customer_ids
    customers_without_orders = len(valid_customer_ids - customers_with_orders)

    injection_total = (
        NULL_EMAIL_COUNT
        + DUPLICATE_CUSTOMER_ID_COUNT
        + NULL_ORDER_CUSTOMER_ID_COUNT
        + NULL_ORDER_PRODUCT_ID_COUNT
        + INVALID_ORDER_CUSTOMER_ID_COUNT
        + INVALID_ORDER_PRODUCT_ID_COUNT
        + DUPLICATE_ORDER_ID_COUNT
    )

    metrics = {
        "valid": len(errors) == 0,
        "errors": errors,
        "row_counts": {
            "customers": len(customers),
            "products": len(products),
            "orders": len(orders),
        },
        "file_sizes_kb": {
            "customers": _file_size_kb(DATA_DIR / "customers.csv"),
            "products": _file_size_kb(DATA_DIR / "products.csv"),
            "orders": _file_size_kb(DATA_DIR / "orders.csv"),
        },
        "null_email_count": null_email_count,
        "duplicate_customer_id_rows": dup_customer_count,
        "null_order_customer_id": null_order_customer,
        "null_order_product_id": null_order_product,
        "invalid_customer_id_count": invalid_customer_fk,
        "invalid_product_id_count": invalid_product_fk,
        "duplicate_order_id_rows": dup_order_count,
        "injection_total": injection_total,
        "distinct_issue_rows_orders": len(
            set(plan.null_order_customer_indices)
            | set(plan.null_order_product_indices)
            | set(plan.invalid_order_customer_indices)
            | set(plan.invalid_order_product_indices)
            | set(plan.duplicate_order_id_indices)
        ),
        "customers_with_orders": len(customers_with_orders),
        "customers_without_orders": customers_without_orders,
        "random_seed": RANDOM_SEED,
        "output_path": str(DATA_DIR),
    }
    return metrics


def print_generation_summary(metrics: dict[str, Any]) -> None:
    """Print computed generation summary to stdout."""
    print("=" * 60)
    print("E-COMMERCE SAMPLE DATA GENERATION SUMMARY")
    print("=" * 60)
    print(f"Random seed:        {metrics['random_seed']}")
    print(f"Output path:        {metrics['output_path']}")
    print()
    print("Row counts:")
    for entity, count in metrics["row_counts"].items():
        print(f"  {entity:12} {count:,}")
    print()
    print("File sizes (KB):")
    for entity, size in metrics["file_sizes_kb"].items():
        print(f"  {entity:12} {size:,.1f}")
    print()
    print("Intentional quality issues (actual):")
    print(f"  NULL customer emails:           {metrics['null_email_count']}")
    print(f"  Duplicate customer_id rows:     {metrics['duplicate_customer_id_rows']}")
    print(f"  NULL order customer_id:         {metrics['null_order_customer_id']}")
    print(f"  NULL order product_id:          {metrics['null_order_product_id']}")
    print(f"  Invalid order customer_id:      {metrics['invalid_customer_id_count']}")
    print(f"  Invalid order product_id:       {metrics['invalid_product_id_count']}")
    print(f"  Duplicate order_id rows:        {metrics['duplicate_order_id_rows']}")
    print(f"  Injection total (listed):       {metrics['injection_total']}")
    print(f"  Distinct order rows w/ issues:  {metrics['distinct_issue_rows_orders']}")
    print()
    print("Distribution notes:")
    print(f"  Customers with >=1 valid order: {metrics['customers_with_orders']:,}")
    print(f"  Customers with zero orders:     {metrics['customers_without_orders']:,}")
    print()
    if metrics["valid"]:
        print("Validation: PASSED")
    else:
        print("Validation: FAILED")
        for err in metrics["errors"]:
            print(f"  - {err}")
    print("=" * 60)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, na_rep="")


def main() -> int:
    """Generate datasets, write CSVs, validate, and print summary."""
    rng, fake = _setup_random(RANDOM_SEED)
    plan = build_issue_plan(rng)

    try:
        customers = generate_customers(rng, fake)
        customers = inject_customer_quality_issues(customers, plan)

        products = generate_products(rng, fake)
        orders = generate_orders(rng, fake, customers, products)
        orders = inject_order_quality_issues(orders, plan)

        _write_csv(customers, DATA_DIR / "customers.csv")
        _write_csv(products, DATA_DIR / "products.csv")
        _write_csv(orders, DATA_DIR / "orders.csv")

        metrics = validate_generated_data(customers, products, orders, plan)
        print_generation_summary(metrics)
        return 0 if metrics["valid"] else 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
