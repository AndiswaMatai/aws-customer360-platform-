"""
Lambda: Kinesis Clickstream Processor

Triggered by the Kinesis event source mapping in
terraform/streaming_and_compute.tf (aws_lambda_event_source_mapping).
Batches up to 500 clickstream records (or 30 seconds, whichever comes
first — see maximum_batching_window_in_seconds in the Terraform config)
and writes them as a Parquet file to the S3 raw zone, where the Glue
Crawler (terraform/glue.tf) picks them up on its next scheduled scan.

This is the real-time leg of the platform — it lands events from POS
terminals / web clients into the same raw/ prefix that the batch
generate_sample_data.py writes to locally, so staged_transform.py
processes both paths identically regardless of how data arrived.
"""
import base64
import json
import os
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
RAW_BUCKET = os.environ["RAW_BUCKET"]


def handler(event, context):
    records = []
    for record in event["Records"]:
        payload = base64.b64decode(record["kinesis"]["data"])
        try:
            data = json.loads(payload)
            records.append(data)
        except json.JSONDecodeError:
            print(f"Skipping malformed record: {record['kinesis']['sequenceNumber']}")
            continue

    if not records:
        return {"statusCode": 200, "recordsProcessed": 0}

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"clickstream_events/realtime_batch_{timestamp}.json"

    body = "\n".join(json.dumps(r) for r in records)  # newline-delimited JSON, Glue-readable

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote {len(records)} records to s3://{RAW_BUCKET}/{key}")
    return {"statusCode": 200, "recordsProcessed": len(records), "s3Key": key}
