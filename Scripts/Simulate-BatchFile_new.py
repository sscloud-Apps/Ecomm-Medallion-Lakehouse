"""Split the static Olist dump into monthly batches to simulate incremental loads.

Layout produced:
    <out>/<YYYY-MM>/olist_orders_dataset.csv          orders purchased in that month
    <out>/<YYYY-MM>/olist_order_items_dataset.csv     + their items, payments, reviews
    <out>/<first month>/                              also gets all reference tables
    <out>/<change month>/olist_products_dataset.csv   200 products with a new category/weight
                                                      (drives the SCD2 versions in gold.dim_product)

Usage:
    python Scripts/Simulate-BatchFile_new.py --raw data/raw --out data/batches [--change-month 2018-01]
"""
import argparse
import csv
import os
import random
import shutil
from contextlib import ExitStack

REFERENCE_FILES = [
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]
ORDER_CHILDREN = [
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
]


def read_csv_rows(path: str) -> tuple[list[str], list[dict[str, str]]]:
    with open(path, newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return reader.fieldnames, list(reader)


def write_month_orders(raw_dir: str, out_dir: str) -> tuple[list[str], dict[str, str]]:
    source_path = os.path.join(raw_dir, "olist_orders_dataset.csv")
    fieldnames, orders = read_csv_rows(source_path)
    orders_by_month: dict[str, list[dict[str, str]]] = {}
    month_by_order_id: dict[str, str] = {}

    for row in orders:
        month = (row.get("order_purchase_timestamp") or "")[:7]
        if not month:
            continue
        orders_by_month.setdefault(month, []).append(row)
        order_id = row.get("order_id")
        if order_id:
            month_by_order_id[order_id] = month

    months = sorted(orders_by_month)
    if not months:
        raise ValueError(f"No orders with a purchase month found in {source_path}")

    for month in months:
        month_dir = os.path.join(out_dir, month)
        os.makedirs(month_dir, exist_ok=True)
        output_path = os.path.join(month_dir, "olist_orders_dataset.csv")
        with open(output_path, "w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(orders_by_month[month])

    return months, month_by_order_id


def split_order_child(
    raw_dir: str,
    out_dir: str,
    name: str,
    months: list[str],
    month_by_order_id: dict[str, str],
) -> None:
    source_path = os.path.join(raw_dir, name)
    with open(source_path, newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {source_path}")

        with ExitStack() as stack:
            writers: dict[str, csv.DictWriter] = {}
            for month in months:
                output_path = os.path.join(out_dir, month, name)
                output = stack.enter_context(
                    open(output_path, "w", newline="", encoding="utf-8")
                )
                writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
                writer.writeheader()
                writers[month] = writer

            for row in reader:
                month = month_by_order_id.get(row.get("order_id") or "")
                if month is not None:
                    writers[month].writerow(row)


def inject_product_changes(raw_dir: str, out_dir: str, month: str) -> None:
    source_path = os.path.join(raw_dir, "olist_products_dataset.csv")
    fieldnames, products = read_csv_rows(source_path)
    if len(products) < 200:
        raise ValueError(f"Expected at least 200 products in {source_path}")

    categories = list(
        dict.fromkeys(
            row["product_category_name"]
            for row in products
            if row.get("product_category_name")
        )
    )
    if not categories:
        raise ValueError(f"No product categories found in {source_path}")

    changed = random.Random(42).sample(products, 200)
    new_categories = random.Random(1).choices(categories, k=len(changed))
    for product, category in zip(changed, new_categories):
        product["product_category_name"] = category
        try:
            weight = float(product.get("product_weight_g") or "")
        except ValueError:
            product["product_weight_g"] = ""
        else:
            product["product_weight_g"] = str(round(weight * 1.1))

    output_path = os.path.join(out_dir, month, "olist_products_dataset.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(changed)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="folder with the 9 Olist CSV files")
    ap.add_argument("--out", default="data/batches")
    ap.add_argument("--change-month", default="2018-01", help="month that receives changed products")
    args = ap.parse_args()

    months, month_by_order_id = write_month_orders(args.raw, args.out)
    for month in months:
        order_count = sum(
            assigned_month == month for assigned_month in month_by_order_id.values()
        )
        print(f"{month}: {order_count:>6} orders")

    for name in ORDER_CHILDREN:
        split_order_child(args.raw, args.out, name, months, month_by_order_id)

    # Reference tables arrive once, with the first batch.
    first_dir = os.path.join(args.out, months[0])
    for name in REFERENCE_FILES:
        shutil.copy(os.path.join(args.raw, name), os.path.join(first_dir, name))

    # Simulate product master-data changes in a later batch (for SCD2).
    if args.change_month in months and args.change_month != months[0]:
        inject_product_changes(args.raw, args.out, args.change_month)
        print(f"{args.change_month}: injected 200 changed products")


if __name__ == "__main__":
    main()