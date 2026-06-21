"""
AWS Glue ETL Job: staged -> curated (Customer 360)

Mirrors engine/medallion_pipeline.py::curated_customer_360() exactly —
joins clickstream funnel activity with completed-order spend per customer,
producing the table Redshift Spectrum exposes for QuickSight/Power BI.

Deployed via terraform/glue.tf (aws_glue_job.curated_transform).
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "staged_database", "curated_bucket"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

STAGED_DB = args["staged_database"]
CURATED_BUCKET = args["curated_bucket"]

customers = glueContext.create_dynamic_frame.from_catalog(database=STAGED_DB, table_name="customers").toDF()
clickstream = glueContext.create_dynamic_frame.from_catalog(database=STAGED_DB, table_name="clickstream_events").toDF()
transactions = glueContext.create_dynamic_frame.from_catalog(database=STAGED_DB, table_name="transactions").toDF()
products = glueContext.create_dynamic_frame.from_catalog(database=STAGED_DB, table_name="products").toDF()

# ── Funnel metrics per customer ─────────────────────────────────────────────
funnel = (
    clickstream.groupBy("customer_id")
    .pivot("event_type", ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"])
    .count()
    .na.fill(0)
)

# ── Spend metrics per customer (completed orders only) ──────────────────────
spend = (
    transactions.filter(F.col("status") == "completed")
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_order_value"),
    )
)

customer_360 = (
    customers
    .join(funnel, "customer_id", "left")
    .join(spend, "customer_id", "left")
    .na.fill(0, subset=["total_orders", "total_spend", "avg_order_value"])
    .withColumn(
        "conversion_rate",
        F.round(F.when(F.col("product_view") > 0, F.col("purchase") / F.col("product_view")).otherwise(0), 4)
    )
)

customer_360.write.mode("overwrite").parquet(f"s3://{CURATED_BUCKET}/customer_360/")
print(f"customer_360: {customer_360.count():,} profiles written")

# ── Category performance ────────────────────────────────────────────────────
category_perf = (
    transactions.filter(F.col("status") == "completed")
    .join(products.select("product_id", "category"), "product_id", "left")
    .groupBy("category")
    .agg(F.sum("amount").alias("revenue"), F.count("order_id").alias("orders"))
    .orderBy(F.desc("revenue"))
)
category_perf.write.mode("overwrite").parquet(f"s3://{CURATED_BUCKET}/category_performance/")

# ── Funnel drop-off (overall) ───────────────────────────────────────────────
funnel_totals = clickstream.groupBy("event_type").count().withColumnRenamed("count", "event_count")
funnel_totals.write.mode("overwrite").parquet(f"s3://{CURATED_BUCKET}/funnel_dropoff/")

job.commit()
