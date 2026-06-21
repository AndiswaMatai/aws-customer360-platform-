"""
Local pipeline engine for the AWS Customer 360 Platform. Mirrors the AWS
Glue ETL jobs in glue_jobs/ exactly — same logic, pandas instead of PySpark,
so the full pipeline is runnable and testable without an AWS account.

S3 zone naming follows the AWS Well-Architected data lake convention:
  raw/      — landed exactly as received (Bronze equivalent)
  staged/   — cleansed, validated, deduplicated (Silver equivalent)
  curated/  — business-ready Customer 360 aggregates (Gold equivalent)
"""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RAW     = Path(__file__).resolve().parent.parent / "data" / "raw"
STAGED_RAW = Path(__file__).resolve().parent.parent / "data" / "lake" / "raw"
STAGED  = Path(__file__).resolve().parent.parent / "data" / "lake" / "staged"
CURATED = Path(__file__).resolve().parent.parent / "data" / "lake" / "curated"
for p in [STAGED_RAW, STAGED, CURATED]:
    p.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 50_000

def now(): return datetime.now(timezone.utc).isoformat()


# ── RAW (S3 landing, Glue Crawler catalogs this automatically) ─────────────
# AWS equivalent: S3 PUT from Kinesis Firehose (clickstream) / Glue Crawler
# scheduled scan (transactions) registers the schema in Glue Data Catalog.
def raw_ingest(table_name: str):
    src = RAW / f"{table_name}.csv"
    dst = STAGED_RAW / f"{table_name}.csv"
    total = 0
    first = True
    for chunk in pd.read_csv(src, chunksize=CHUNK_SIZE, dtype=str):
        chunk["_ingested_ts"] = now()
        chunk.to_csv(dst, mode="w" if first else "a", header=first, index=False)
        first = False
        total += len(chunk)
    return total


# ── STAGED (Glue ETL job: cleanse, validate, dedupe) ────────────────────────
# AWS equivalent: glue_jobs/staged_clickstream.py running as a Glue job
#   dyf = glueContext.create_dynamic_frame.from_catalog(database="raw", table_name="clickstream_events")
#   df = dyf.toDF().filter(...).dropDuplicates(["event_id"])
#   glueContext.write_dynamic_frame.from_options(DynamicFrame.fromDF(df, glueContext, "staged"), ...)
def staged_clean_clickstream():
    rejected, clean_chunks, seen = 0, [], set()
    for chunk in pd.read_csv(STAGED_RAW / "clickstream_events.csv", chunksize=CHUNK_SIZE):
        before = len(chunk)
        chunk = chunk[chunk["customer_id"].notna() & (chunk["customer_id"] != "")]
        chunk = chunk[~chunk["event_id"].isin(seen)].drop_duplicates(subset=["event_id"])
        seen.update(chunk["event_id"])
        rejected += before - len(chunk)
        clean_chunks.append(chunk)
    clean = pd.concat(clean_chunks, ignore_index=True)
    clean["_cleansed_ts"] = now()
    clean.to_csv(STAGED / "clickstream_events.csv", index=False)
    return len(clean), rejected


def staged_clean_transactions(valid_customer_ids: set):
    rejected, clean_chunks = 0, []
    for chunk in pd.read_csv(STAGED_RAW / "transactions.csv", chunksize=CHUNK_SIZE):
        before = len(chunk)
        chunk["quantity"] = pd.to_numeric(chunk["quantity"], errors="coerce")
        chunk["amount"] = pd.to_numeric(chunk["amount"], errors="coerce")
        chunk = chunk[chunk["quantity"] > 0]
        chunk = chunk[chunk["amount"] > 0]
        chunk = chunk[chunk["customer_id"].isin(valid_customer_ids)]
        rejected += before - len(chunk)
        clean_chunks.append(chunk)
    clean = pd.concat(clean_chunks, ignore_index=True)
    clean["_cleansed_ts"] = now()
    clean.to_csv(STAGED / "transactions.csv", index=False)
    return len(clean), rejected


def staged_pass_through(table_name: str):
    df = pd.read_csv(STAGED_RAW / f"{table_name}.csv")
    df.to_csv(STAGED / f"{table_name}.csv", index=False)
    return len(df)


# ── CURATED (Glue ETL job: Customer 360 join + aggregates, loaded to Redshift) ──
# AWS equivalent: glue_jobs/curated_customer360.py writes Parquet to the
# curated S3 prefix, then a Redshift COPY (or Redshift Spectrum external
# table) makes it queryable for analysts and QuickSight/Power BI.
def curated_customer_360():
    customers = pd.read_csv(STAGED / "customers.csv")
    clickstream = pd.read_csv(STAGED / "clickstream_events.csv")
    transactions = pd.read_csv(STAGED / "transactions.csv")
    products = pd.read_csv(STAGED / "products.csv")

    # Funnel metrics per customer
    funnel = clickstream.groupby(["customer_id", "event_type"]).size().unstack(fill_value=0).reset_index()
    for col in ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"]:
        if col not in funnel.columns:
            funnel[col] = 0

    # Spend metrics per customer (completed orders only)
    completed = transactions[transactions["status"] == "completed"]
    spend = completed.groupby("customer_id").agg(
        total_orders=("order_id", "count"),
        total_spend=("amount", "sum"),
        avg_order_value=("amount", "mean"),
    ).reset_index()
    spend["avg_order_value"] = spend["avg_order_value"].round(2)

    customer_360 = customers.merge(funnel, on="customer_id", how="left") \
                             .merge(spend, on="customer_id", how="left")
    customer_360[["total_orders", "total_spend", "avg_order_value"]] = \
        customer_360[["total_orders", "total_spend", "avg_order_value"]].fillna(0)

    # Conversion rate: purchases / product_views — core funnel KPI
    customer_360["conversion_rate"] = (
        customer_360["purchase"] / customer_360["product_view"].replace(0, pd.NA)
    ).fillna(0).round(4)

    customer_360.to_csv(CURATED / "customer_360.csv", index=False)

    # Category performance (joins transactions -> products)
    txn_with_category = completed.merge(products[["product_id", "category"]], on="product_id", how="left")
    category_perf = txn_with_category.groupby("category").agg(
        revenue=("amount", "sum"), orders=("order_id", "count"),
    ).reset_index().sort_values("revenue", ascending=False)
    category_perf.to_csv(CURATED / "category_performance.csv", index=False)

    # Funnel drop-off by stage (overall, not per-customer)
    funnel_totals = clickstream["event_type"].value_counts().reindex(
        ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"]
    ).fillna(0).astype(int).reset_index()
    funnel_totals.columns = ["stage", "event_count"]
    funnel_totals.to_csv(CURATED / "funnel_dropoff.csv", index=False)

    return customer_360, category_perf, funnel_totals


def main():
    print("=" * 60)
    print("AWS CUSTOMER 360 PLATFORM — RAW / STAGED / CURATED PIPELINE")
    print("=" * 60)

    tables = ["customers", "products", "clickstream_events", "transactions"]
    print("\n[Raw] S3 landing zone ingest...")
    for t in tables:
        n = raw_ingest(t)
        print(f"   {t}: {n:,} rows")

    print("\n[Staged] Glue ETL cleansing...")
    ck_clean, ck_rejected = staged_clean_clickstream()
    print(f"   clickstream_events: {ck_clean:,} clean ({ck_rejected:,} rejected — missing customer_id)")
    n_cust = staged_pass_through("customers")
    n_prod = staged_pass_through("products")
    valid_customers = set(pd.read_csv(STAGED / "customers.csv", usecols=["customer_id"])["customer_id"])
    txn_clean, txn_rejected = staged_clean_transactions(valid_customers)
    print(f"   transactions: {txn_clean:,} clean ({txn_rejected:,} rejected)")

    print("\n[Curated] Customer 360 join + aggregates...")
    c360, category, funnel = curated_customer_360()
    print(f"   customer_360: {len(c360):,} customer profiles")
    print(f"   category_performance: {len(category)} categories")

    print("\n" + "=" * 60)
    print("CONVERSION FUNNEL (all customers)")
    print("=" * 60)
    print(funnel.to_string(index=False))

    print("\nTop categories by revenue:")
    print(category.head(5).to_string(index=False))

    avg_conversion = c360[c360["product_view"] > 0]["conversion_rate"].mean()
    print(f"\nAverage customer conversion rate: {avg_conversion:.2%}")


if __name__ == "__main__":
    main()
