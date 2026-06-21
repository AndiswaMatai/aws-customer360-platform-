"""
Runs the data quality suite against the staged layer produced by
engine/medallion_pipeline.py. Run after the pipeline:

    python engine/medallion_pipeline.py
    python data_quality/run_dq_suite.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from dq_framework import check_completeness, check_uniqueness, check_referential_integrity, check_freshness, run_suite

STAGED = Path(__file__).resolve().parent.parent / "data" / "lake" / "staged"


def main():
    transactions = pd.read_csv(STAGED / "transactions.csv")
    customers = pd.read_csv(STAGED / "customers.csv")
    clickstream = pd.read_csv(STAGED / "clickstream_events.csv")
    valid_customer_ids = set(customers["customer_id"])

    checks = [
        check_completeness(transactions, "transactions", ["customer_id", "product_id", "amount"]),
        check_uniqueness(transactions, "transactions", "order_id"),
        check_referential_integrity(transactions, "transactions", "customer_id", valid_customer_ids),
        check_freshness(transactions, "transactions", "order_date", max_age_days=3650),
        check_uniqueness(customers, "customers", "customer_id"),
        check_uniqueness(clickstream, "clickstream_events", "event_id"),
        check_referential_integrity(clickstream, "clickstream_events", "customer_id", valid_customer_ids),
    ]

    results, failed = run_suite(checks)

    print("=" * 60)
    print("DATA QUALITY SUITE RESULTS")
    print("=" * 60)
    for _, row in results.iterrows():
        flag = "✓" if row["status"] == "PASS" else "✗"
        print(f"  [{flag}] {row['table']}.{row['check_name']}: {row['metric_value']} (threshold {row['threshold']})")
        print(f"        {row['detail']}")

    print(f"\n{len(failed)} of {len(results)} checks failed.")
    if len(failed) > 0:
        print("\nIn production: this would fail the Glue job step in Step Functions")
        print("and trigger the CloudWatch alarm defined in monitoring/alarms.tf")
        sys.exit(1)
    else:
        print("All checks passed — curated layer is safe to load into Redshift.")


if __name__ == "__main__":
    main()
