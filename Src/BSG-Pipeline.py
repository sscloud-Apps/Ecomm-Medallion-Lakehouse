"""Olist medallion pipeline: bronze -> silver -> gold with PySpark + Delta Lake.

Run every batch in order:   python olist_pipeline.py --batches-dir data/batches
Run a single batch:         python olist_pipeline.py --batches-dir data/batches --batch 2017-01
"""
import argparse
import os
from datetime import datetime
from typing import List, Optional

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LAKE = os.path.abspath("lake")
META = ["_ingest_ts", "_batch_id"]

SOURCES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


# ======================================================================
# Helpers
# ======================================================================
def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("olist-medallion")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", LAKE)
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for schema in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return spark


def log_dq(spark, batch_id, layer, table, rows_in, rows_out, rows_quarantined=0) -> None:
    """Append one row to ops.dq_log (row counts per layer/table/batch)."""
    spark.createDataFrame(
        [(batch_id, layer, table, int(rows_in), int(rows_out), int(rows_quarantined), datetime.now())],
        "batch_id string, layer string, tbl string, rows_in long, rows_out long, "
        "rows_quarantined long, logged_at timestamp",
    ).write.format("delta").mode("append").saveAsTable("ops.dq_log")


def upsert(spark, df: DataFrame, table: str, keys: List[str]) -> None:
    """MERGE df into a Delta table on `keys` (creates the table on first use)."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def overwrite(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def latest_per_key(df: DataFrame, keys: List[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def date_key(col: str) -> F.Column:
    """Timestamp/date column -> yyyyMMdd integer (NULL stays NULL)."""
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


# ======================================================================
# Bronze: raw strings + audit columns, append-only, idempotent per batch
# ======================================================================
def ingest_bronze(spark, batch_dir: str, batch_id: str) -> None:
    for name, fname in SOURCES.items():
        path = os.path.join(batch_dir, fname)
        if not os.path.exists(path):
            continue
        df = (
            spark.read.option("header", True)
            .option("multiLine", True)  # review comments contain line breaks
            .option("quote", '"')
            .option("escape", '"')
            .csv(path)  # no inferSchema: everything stays a string
        )
        df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])  # strip BOM
        df = (
            df.withColumn("_ingest_ts", F.current_timestamp())
            # On newer Spark / Unity Catalog use the _metadata.file_path column instead.
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(batch_id))
        )
        table = f"bronze.{name}"
        n = df.count()
        if spark.catalog.tableExists(table):
            DeltaTable.forName(spark, table).delete(F.col("_batch_id") == batch_id)  # re-run safe
            df.write.format("delta").mode("append").saveAsTable(table)
        else:
            df.write.format("delta").saveAsTable(table)
        log_dq(spark, batch_id, "bronze", name, n, n)
        print(f"  bronze.{name}: {n} rows")


# ======================================================================
# Silver: typed, validated, deduplicated, MERGE-upserted
# ======================================================================
def bronze(spark, name: str, batch_id: str) -> Optional[DataFrame]:
    table = f"bronze.{name}"
    if not spark.catalog.tableExists(table):
        return None
    df = spark.table(table).filter(F.col("_batch_id") == batch_id)
    return None if df.isEmpty() else df


def quarantine(spark, rejected: DataFrame, name: str, reason: str, batch_id: str) -> int:
    n = rejected.count()
    table = f"silver.quarantine_{name}"
    if spark.catalog.tableExists(table):
        DeltaTable.forName(spark, table).delete(F.col("_batch_id") == batch_id)  # re-run safe
    if n:
        (
            rejected.withColumn("_reject_reason", F.lit(reason))
            .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
        )
    return n


def run_silver_table(spark, batch_id, name, typed: DataFrame, keys: List[str], rule: str) -> None:
    """validate -> quarantine bad rows -> dedupe -> MERGE -> log counts."""
    total = typed.count()
    ok = F.coalesce(F.expr(rule), F.lit(False))
    bad = quarantine(spark, typed.filter(~ok), name, rule, batch_id)
    clean = latest_per_key(typed.filter(ok), keys).cache()
    kept = clean.count()
    upsert(spark, clean, f"silver.{name}", keys)
    clean.unpersist()
    log_dq(spark, batch_id, "silver", name, total, kept, bad)
    print(f"  silver.{name}: {kept} kept, {bad} quarantined")


def build_silver(spark, b: str) -> None:
    # Reference data first (only present in the first batch).
    raw = bronze(spark, "category_translation", b)
    if raw is not None:
        typed = raw.select(
            F.col("product_category_name").alias("category_pt"),
            F.col("product_category_name_english").alias("category_en"),
            *META,
        )
        run_silver_table(spark, b, "category_translation", typed, ["category_pt"], "category_pt IS NOT NULL")

    raw = bronze(spark, "customers", b)
    if raw is not None:
        typed = raw.select(
            "customer_id",
            "customer_unique_id",
            F.lpad("customer_zip_code_prefix", 5, "0").alias("zip_prefix"),
            F.initcap(F.trim("customer_city")).alias("city"),
            F.upper(F.trim("customer_state")).alias("state"),
            *META,
        )
        run_silver_table(spark, b, "customer", typed, ["customer_id"],
                         "customer_id IS NOT NULL AND customer_unique_id IS NOT NULL")

    raw = bronze(spark, "sellers", b)
    if raw is not None:
        typed = raw.select(
            "seller_id",
            F.lpad("seller_zip_code_prefix", 5, "0").alias("zip_prefix"),
            F.initcap(F.trim("seller_city")).alias("city"),
            F.upper(F.trim("seller_state")).alias("state"),
            *META,
        )
        run_silver_table(spark, b, "seller", typed, ["seller_id"], "seller_id IS NOT NULL")

    raw = bronze(spark, "geolocation", b)
    if raw is not None:
        typed = raw.select(
            F.lpad("geolocation_zip_code_prefix", 5, "0").alias("zip_prefix"),
            F.col("geolocation_lat").cast("double").alias("lat"),
            F.col("geolocation_lng").cast("double").alias("lng"),
            *META,
        )
        inside = F.coalesce(F.expr("lat BETWEEN -34 AND 6 AND lng BETWEEN -74 AND -34"), F.lit(False))
        quarantine(spark, typed.filter(~inside), "geolocation", "coordinates outside Brazil", b)
        collapsed = typed.filter(inside).groupBy("zip_prefix").agg(
            F.avg("lat").alias("lat"),
            F.avg("lng").alias("lng"),
            F.count("*").alias("n_points"),
            F.max("_ingest_ts").alias("_ingest_ts"),
            F.first("_batch_id").alias("_batch_id"),
        )
        run_silver_table(spark, b, "geolocation", collapsed, ["zip_prefix"], "zip_prefix IS NOT NULL")

    raw = bronze(spark, "products", b)
    if raw is not None:
        tr = spark.table("silver.category_translation").select("category_pt", "category_en")
        typed = (
            raw.alias("p")
            .join(F.broadcast(tr.alias("t")), F.col("p.product_category_name") == F.col("t.category_pt"), "left")
            .select(
                F.col("p.product_id").alias("product_id"),
                F.col("p.product_category_name").alias("category_pt"),
                F.coalesce(F.col("t.category_en"), F.col("p.product_category_name"), F.lit("unknown"))
                .alias("category_en"),
                F.col("p.product_photos_qty").cast("int").alias("photos_qty"),
                F.col("p.product_name_lenght").cast("int").alias("name_length"),  # sic (source typo)
                F.col("p.product_description_lenght").cast("int").alias("description_length"),
                F.col("p.product_weight_g").cast("int").alias("weight_g"),
                F.col("p.product_length_cm").cast("int").alias("length_cm"),
                F.col("p.product_height_cm").cast("int").alias("height_cm"),
                F.col("p.product_width_cm").cast("int").alias("width_cm"),
                F.col("p._ingest_ts").alias("_ingest_ts"),
                F.col("p._batch_id").alias("_batch_id"),
            )
        )
        run_silver_table(spark, b, "product", typed, ["product_id"],
                         "product_id IS NOT NULL AND (weight_g IS NULL OR weight_g >= 0)")

    raw = bronze(spark, "orders", b)
    if raw is not None:
        typed = raw.select(
            "order_id",
            "customer_id",
            F.lower(F.trim("order_status")).alias("order_status"),
            F.to_timestamp("order_purchase_timestamp").alias("purchase_ts"),
            F.to_timestamp("order_approved_at").alias("approved_ts"),
            F.to_timestamp("order_delivered_carrier_date").alias("delivered_carrier_ts"),
            F.to_timestamp("order_delivered_customer_date").alias("delivered_customer_ts"),
            F.to_timestamp("order_estimated_delivery_date").alias("estimated_delivery_ts"),
            *META,
        )
        run_silver_table(spark, b, "order", typed, ["order_id"],
                         "order_id IS NOT NULL AND customer_id IS NOT NULL AND purchase_ts IS NOT NULL")

    raw = bronze(spark, "order_items", b)
    if raw is not None:
        typed = raw.select(
            "order_id",
            F.col("order_item_id").cast("int").alias("order_item_id"),
            "product_id",
            "seller_id",
            F.to_timestamp("shipping_limit_date").alias("shipping_limit_ts"),
            F.col("price").cast("decimal(12,2)").alias("price"),
            F.col("freight_value").cast("decimal(12,2)").alias("freight_value"),
            *META,
        )
        run_silver_table(
            spark, b, "order_item", typed, ["order_id", "order_item_id"],
            "order_id IS NOT NULL AND order_item_id IS NOT NULL AND product_id IS NOT NULL "
            "AND seller_id IS NOT NULL AND price >= 0 AND freight_value >= 0",
        )

    raw = bronze(spark, "order_payments", b)
    if raw is not None:
        typed = raw.select(
            "order_id",
            F.col("payment_sequential").cast("int").alias("payment_sequential"),
            F.lower(F.trim("payment_type")).alias("payment_type"),
            F.col("payment_installments").cast("int").alias("payment_installments"),
            F.col("payment_value").cast("decimal(12,2)").alias("payment_value"),
            *META,
        )
        run_silver_table(spark, b, "order_payment", typed, ["order_id", "payment_sequential"],
                         "order_id IS NOT NULL AND payment_sequential IS NOT NULL AND payment_value >= 0")

    raw = bronze(spark, "order_reviews", b)
    if raw is not None:
        typed = raw.select(
            "review_id",
            "order_id",
            F.col("review_score").cast("int").alias("review_score"),
            F.col("review_comment_title").alias("comment_title"),
            F.col("review_comment_message").alias("comment_message"),
            F.to_timestamp("review_creation_date").alias("creation_ts"),
            F.to_timestamp("review_answer_timestamp").alias("answer_ts"),
            *META,
        )
        run_silver_table(spark, b, "order_review", typed, ["review_id", "order_id"],
                         "review_id IS NOT NULL AND order_id IS NOT NULL AND review_score BETWEEN 1 AND 5")


# ======================================================================
# Gold: star schema
# ======================================================================
def build_dim_date(spark) -> None:
    df = spark.sql(
        "SELECT explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day)) AS date"
    )
    df = df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
    overwrite(df, "gold.dim_date")


def build_dim_customer(spark) -> None:
    """SCD1 at customer_unique_id grain, using the location of the customer's latest order."""
    cust = spark.table("silver.customer")
    orders = spark.table("silver.order").select("customer_id", "purchase_ts")
    geo = spark.table("silver.geolocation").select(
        F.col("zip_prefix").alias("g_zip"), F.col("lat"), F.col("lng")
    )
    w = Window.partitionBy("customer_unique_id").orderBy(F.col("purchase_ts").desc_nulls_last())
    latest = (
        cust.join(orders, "customer_id", "left")
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
    )
    dim = (
        latest.join(geo, latest["zip_prefix"] == geo["g_zip"], "left")
        .select(
            F.xxhash64("customer_unique_id").alias("customer_key"),
            "customer_unique_id",
            "zip_prefix",
            "city",
            "state",
            "lat",
            "lng",
        )
    )
    overwrite(dim, "gold.dim_customer")


def build_dim_seller(spark) -> None:
    sel = spark.table("silver.seller")
    geo = spark.table("silver.geolocation").select(
        F.col("zip_prefix").alias("g_zip"), F.col("lat"), F.col("lng")
    )
    dim = (
        sel.join(geo, sel["zip_prefix"] == geo["g_zip"], "left")
        .select(F.xxhash64("seller_id").alias("seller_key"), "seller_id", "zip_prefix", "city", "state", "lat", "lng")
    )
    overwrite(dim, "gold.dim_seller")


def scd2_merge(spark, src: DataFrame, target: str, key: str, sk: str, tracked: List[str],
               effective_from: str, initial_effective_from: str = "1900-01-01") -> None:
    """Type-2 slowly changing dimension via one Delta MERGE.

    src          one row per `key`, business attributes only
    tracked      attributes whose change creates a new version
    effective_from  start of validity for new versions created by this run
    The surrogate key is xxhash64(key, effective_from): deterministic, one per version.
    """
    src = src.withColumn(
        "_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked]), 256),
    )
    attrs = src.columns

    if not spark.catalog.tableExists(target):  # initial load: version 1 covers all history
        first = (
            src.withColumn("effective_from", F.lit(initial_effective_from).cast("timestamp"))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        first = first.withColumn(sk, F.xxhash64(F.col(key), F.col("effective_from")))
        first.write.format("delta").saveAsTable(target)
        return

    current = spark.table(target).filter("is_current").select(key, F.col("_hash").alias("_t_hash"))
    joined = src.join(current, key, "left")
    new_rows = joined.filter(F.col("_t_hash").isNull())
    changed = joined.filter(F.col("_t_hash").isNotNull() & (F.col("_t_hash") != F.col("_hash")))

    key_type = src.schema[key].dataType
    to_insert = new_rows.unionByName(changed).drop("_t_hash").withColumn("_merge_key", F.lit(None).cast(key_type))
    to_close = changed.drop("_t_hash").withColumn("_merge_key", F.col(key))  # matches the current version
    staged = (
        to_close.unionByName(to_insert)
        .withColumn("effective_from", F.lit(effective_from).cast("timestamp"))
        .withColumn("_sk", F.xxhash64(F.col(key), F.col("effective_from")))
    )

    insert_values = {c: F.col(f"s.{c}") for c in attrs + ["effective_from"]}
    insert_values.update({sk: F.col("s._sk"), "effective_to": F.lit(None).cast("timestamp"), "is_current": F.lit(True)})
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": F.lit(False), "effective_to": F.col("s.effective_from")})
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )


def build_dim_product(spark, b: str) -> None:
    src = spark.table("silver.product").filter(F.col("_batch_id") == b)
    if src.isEmpty():
        return
    src = src.select(
        "product_id", "category_pt", "category_en", "weight_g", "length_cm", "height_cm", "width_cm",
        "photos_qty", "name_length", "description_length",
    )
    scd2_merge(
        spark, src, "gold.dim_product", key="product_id", sk="product_key",
        tracked=["category_pt", "category_en", "weight_g", "length_cm", "height_cm", "width_cm"],
        effective_from=f"{b}-01 00:00:00",
    )


def build_fact_order_items(spark, b: str) -> None:
    items = spark.table("silver.order_item").filter(F.col("_batch_id") == b).alias("i")
    orders = spark.table("silver.order").alias("o")
    cust = spark.table("silver.customer").select("customer_id", "customer_unique_id").alias("c")
    prod = spark.table("gold.dim_product").select("product_id", "product_key", "effective_from", "effective_to").alias("p")

    # Pick the product version that was valid when the order was placed.
    in_version = (
        (F.col("i.product_id") == F.col("p.product_id"))
        & (F.col("o.purchase_ts") >= F.col("p.effective_from"))
        & (F.col("p.effective_to").isNull() | (F.col("o.purchase_ts") < F.col("p.effective_to")))
    )
    fact = (
        items.join(orders, F.col("i.order_id") == F.col("o.order_id"))
        .join(cust, F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(prod, in_version, "left")
        .select(
            F.col("i.order_id").alias("order_id"),
            F.col("i.order_item_id").alias("order_item_id"),
            F.coalesce(F.col("p.product_key"), F.lit(-1).cast("long")).alias("product_key"),
            F.xxhash64("i.seller_id").alias("seller_key"),
            F.xxhash64("c.customer_unique_id").alias("customer_key"),
            date_key("o.purchase_ts").alias("purchase_date_key"),
            date_key("o.delivered_customer_ts").alias("delivered_date_key"),
            date_key("o.estimated_delivery_ts").alias("estimated_date_key"),
            F.col("o.order_status").alias("order_status"),
            F.col("i.price").alias("price"),
            F.col("i.freight_value").alias("freight_value"),
            (F.col("i.price") + F.col("i.freight_value")).alias("item_total"),
            F.datediff(F.col("o.delivered_customer_ts"), F.col("o.purchase_ts")).alias("delivery_days"),
            F.datediff(F.col("o.delivered_customer_ts"), F.col("o.estimated_delivery_ts")).alias("delay_days"),
            (F.col("o.delivered_customer_ts") > F.col("o.estimated_delivery_ts")).alias("is_late"),
            F.lit(b).alias("_batch_id"),
        )
    )
    n = fact.count()
    upsert(spark, fact, "gold.fact_order_items", ["order_id", "order_item_id"])
    log_dq(spark, b, "gold", "fact_order_items", n, n)
    print(f"  gold.fact_order_items: {n} rows")


def build_fact_payments(spark, b: str) -> None:
    pay = spark.table("silver.order_payment").filter(F.col("_batch_id") == b).alias("p")
    orders = spark.table("silver.order").alias("o")
    cust = spark.table("silver.customer").select("customer_id", "customer_unique_id").alias("c")
    fact = (
        pay.join(orders, F.col("p.order_id") == F.col("o.order_id"), "left")
        .join(cust, F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .select(
            F.col("p.order_id").alias("order_id"),
            F.col("p.payment_sequential").alias("payment_sequential"),
            F.xxhash64("c.customer_unique_id").alias("customer_key"),
            date_key("o.purchase_ts").alias("purchase_date_key"),
            F.col("p.payment_type").alias("payment_type"),
            F.col("p.payment_installments").alias("payment_installments"),
            F.col("p.payment_value").alias("payment_value"),
            F.lit(b).alias("_batch_id"),
        )
    )
    n = fact.count()
    upsert(spark, fact, "gold.fact_payments", ["order_id", "payment_sequential"])
    log_dq(spark, b, "gold", "fact_payments", n, n)
    print(f"  gold.fact_payments: {n} rows")


def build_fact_reviews(spark, b: str) -> None:
    rev = spark.table("silver.order_review").filter(F.col("_batch_id") == b)
    fact = rev.select(
        "review_id",
        "order_id",
        "review_score",
        date_key("creation_ts").alias("review_creation_date_key"),
        ((F.unix_timestamp("answer_ts") - F.unix_timestamp("creation_ts")) / 3600).alias("response_hours"),
        F.col("comment_message").isNotNull().alias("has_comment"),
        F.lit(b).alias("_batch_id"),
    )
    n = fact.count()
    upsert(spark, fact, "gold.fact_reviews", ["review_id", "order_id"])
    log_dq(spark, b, "gold", "fact_reviews", n, n)
    print(f"  gold.fact_reviews: {n} rows")


def build_aggregates(spark) -> None:
    """Fully rebuilt each run: simple and idempotent at this size."""
    f = spark.table("gold.fact_order_items")
    daily = f.groupBy("purchase_date_key").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
        F.sum("price").alias("gmv"),
        F.sum("freight_value").alias("freight"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.avg(F.col("is_late").cast("int")).alias("late_rate"),
    )
    overwrite(daily, "gold.agg_daily_sales")

    ltv = f.groupBy("customer_key").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
        F.sum("item_total").alias("total_spent"),
        F.min("purchase_date_key").alias("first_purchase_date_key"),
        F.max("purchase_date_key").alias("last_purchase_date_key"),
    ).withColumn("avg_order_value", F.col("total_spent") / F.col("orders"))
    overwrite(ltv, "gold.agg_customer_ltv")


def build_gold(spark, b: str) -> None:
    if not spark.catalog.tableExists("gold.dim_date"):
        build_dim_date(spark)
    build_dim_customer(spark)
    build_dim_seller(spark)
    build_dim_product(spark, b)
    build_fact_order_items(spark, b)
    build_fact_payments(spark, b)
    build_fact_reviews(spark, b)
    build_aggregates(spark)


# ======================================================================
# Orchestration
# ======================================================================
def run_batch(spark, batches_dir: str, batch_id: str) -> None:
    print(f"== batch {batch_id}")
    ingest_bronze(spark, os.path.join(batches_dir, batch_id), batch_id)
    build_silver(spark, batch_id)
    build_gold(spark, batch_id)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batches-dir", required=True)
    ap.add_argument("--batch", help="run a single batch (YYYY-MM); default: all, in order")
    args = ap.parse_args()

    spark = get_spark()
    batches = [args.batch] if args.batch else sorted(
        d for d in os.listdir(args.batches_dir) if os.path.isdir(os.path.join(args.batches_dir, d))
    )
    for b in batches:
        run_batch(spark, args.batches_dir, b)
    spark.stop()


if __name__ == "__main__":
    main()