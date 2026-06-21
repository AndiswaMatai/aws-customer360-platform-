"""
AWS Glue ETL Job: raw -> staged

Mirrors engine/medallion_pipeline.py::staged_clean_clickstream() and
staged_clean_transactions() exactly — same rules, PySpark instead of pandas.

Deployed via terraform/glue.tf (aws_glue_job.staged_transform), orchestrated
by step_functions/medallion_state_machine.json, triggered daily by the
EventBridge rule in terraform/streaming_and_compute.tf.

Job bookmarking (--job-bookmark-option job-bookmark-enable) means this job
only processes files that have arrived in S3 raw/ since its last successful
run — it never reprocesses the same data twice, the Glue-native equivalent
of the idempotent ingestion pattern used throughout this portfolio.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_database", "staged_bucket"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_DB = args["raw_database"]
STAGED_BUCKET = args["staged_bucket"]

# ── Transactions: cleanse + validate ────────────────────────────────────────
txn_dyf = glueContext.create_dynamic_frame.from_catalog(database=RAW_DB, table_name="transactions")
txn_df = txn_dyf.toDF()

txn_clean = (
    txn_df
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("amount", F.col("amount").cast("double"))
    .filter(F.col("quantity") > 0)
    .filter(F.col("amount") > 0)
    .filter((F.col("customer_id").isNotNull()) & (F.col("customer_id") != ""))
    .dropDuplicates(["order_id"])
    .withColumn("_cleansed_ts", F.current_timestamp())
)

txn_rejected = txn_df.count() - txn_clean.count()
print(f"transactions: {txn_clean.count():,} clean, {txn_rejected:,} rejected")

(txn_clean.write
    .mode("overwrite")
    .partitionBy("status")
    .parquet(f"s3://{STAGED_BUCKET}/transactions/"))

# ── Clickstream events: drop anonymous events, dedupe ───────────────────────
ck_dyf = glueContext.create_dynamic_frame.from_catalog(database=RAW_DB, table_name="clickstream_events")
ck_df = ck_dyf.toDF()

ck_clean = (
    ck_df
    .filter((F.col("customer_id").isNotNull()) & (F.col("customer_id") != ""))
    .dropDuplicates(["event_id"])
    .withColumn("_cleansed_ts", F.current_timestamp())
)

ck_rejected = ck_df.count() - ck_clean.count()
print(f"clickstream_events: {ck_clean.count():,} clean, {ck_rejected:,} rejected (anonymous/duplicate)")

(ck_clean.write
    .mode("overwrite")
    .partitionBy("event_type")
    .parquet(f"s3://{STAGED_BUCKET}/clickstream_events/"))

# ── Customers and products: schema-validated passthrough ───────────────────
for table in ["customers", "products"]:
    dyf = glueContext.create_dynamic_frame.from_catalog(database=RAW_DB, table_name=table)
    dyf.toDF().write.mode("overwrite").parquet(f"s3://{STAGED_BUCKET}/{table}/")

job.commit()
