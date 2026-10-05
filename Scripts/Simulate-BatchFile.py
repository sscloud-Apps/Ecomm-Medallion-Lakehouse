"""Split the static Olist dump into monthly batches to simulate incremental loads.

Layout produced:
    <out>/<YYYY-MM>/olist_orders_dataset.csv          orders purchased in that month
    <out>/<YYYY-MM>/olist_order_items_dataset.csv     + their items, payments, reviews
    <out>/<first month>/                              also gets all reference tables
    <out>/<change month>/olist_products_dataset.csv   200 products with a new category/weight
                                                      (drives the SCD2 versions in gold.dim_product)

Usage:
    python simulate_batches.py --raw data/raw --out data/batches [--change-month 2018-01]
"""
import argparse
import os
import shutil

import pandas as pd

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


def read(raw_dir: str, name: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(raw_dir, name), dtype=str)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="folder with the 9 Olist CSV files")
    ap.add_argument("--out", default="data/batches")
    ap.add_argument("--change-month", default="2018-01", help="month that receives changed products")
    args = ap.parse_args()

    orders = read(args.raw, "olist_orders_dataset.csv")
    orders["_month"] = orders["order_purchase_timestamp"].str[:7]
    months = sorted(orders["_month"].dropna().unique())
    children = {name: read(args.raw, name) for name in ORDER_CHILDREN}

    for month in months:
        out_dir = os.path.join(args.out, month)
        os.makedirs(out_dir, exist_ok=True)

        month_orders = orders[orders["_month"] == month].drop(columns="_month")
        month_orders.to_csv(os.path.join(out_dir, "olist_orders_dataset.csv"), index=False)

        ids = set(month_orders["order_id"])
        for name, df in children.items():
            df[df["order_id"].isin(ids)].to_csv(os.path.join(out_dir, name), index=False)

        print(f"{month}: {len(month_orders):>6} orders")

    # Reference tables arrive once, with the first batch.
    first_dir = os.path.join(args.out, months[0])
    for name in REFERENCE_FILES:
        shutil.copy(os.path.join(args.raw, name), os.path.join(first_dir, name))

    # Simulate product master-data changes in a later batch (for SCD2).
    if args.change_month in months and args.change_month != months[0]:
        products = read(args.raw, "olist_products_dataset.csv")
        changed = products.sample(200, random_state=42).copy()
        categories = products["product_category_name"].dropna().unique()
        changed["product_category_name"] = (
            pd.Series(categories).sample(len(changed), replace=True, random_state=1).values
        )
        weight = pd.to_numeric(changed["product_weight_g"], errors="coerce")
        changed["product_weight_g"] = (weight * 1.1).round().astype("Int64").astype("string").fillna("")
        changed.to_csv(
            os.path.join(args.out, args.change_month, "olist_products_dataset.csv"), index=False
        )
        print(f"{args.change_month}: injected {len(changed)} changed products")


if __name__ == "__main__":
    main()