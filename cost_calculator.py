"""
Cost Optimization Calculator — AWS Customer 360 Platform

Models the savings from the cost controls in this platform's Terraform:

  1. S3 lifecycle tiering: Standard -> IA -> Glacier (terraform/s3.tf)
  2. Glue job bookmarking: never reprocess unchanged data (terraform/glue.tf)
  3. Redshift Serverless: pay-per-second compute vs. an always-on cluster
  4. Kinesis batching: fewer Lambda invocations via batch windowing

Run: python cost_optimization/cost_calculator.py
"""
from dataclasses import dataclass

# Approximate af-south-1 (Cape Town) pricing (USD), illustrative for the
# model — always re-check the AWS Pricing Calculator before using in a
# real budget.
S3_STANDARD_GB_MONTH = 0.025
S3_IA_GB_MONTH = 0.0138
S3_GLACIER_GB_MONTH = 0.0045
GLUE_DPU_HOUR = 0.44
REDSHIFT_SERVERLESS_RPU_HOUR = 0.375
REDSHIFT_PROVISIONED_EQUIVALENT_MONTHLY = 1200  # smallest always-on multi-node cluster, illustrative
LAMBDA_REQUEST_COST_PER_MILLION = 0.20
LAMBDA_GBSECOND_COST = 0.0000166667


@dataclass
class StorageProfile:
    total_gb: float
    pct_standard: float
    pct_ia: float
    pct_glacier: float


def s3_cost_without_tiering(profile: StorageProfile) -> float:
    return profile.total_gb * S3_STANDARD_GB_MONTH


def s3_cost_with_tiering(profile: StorageProfile) -> float:
    return (
        profile.total_gb * profile.pct_standard * S3_STANDARD_GB_MONTH
        + profile.total_gb * profile.pct_ia * S3_IA_GB_MONTH
        + profile.total_gb * profile.pct_glacier * S3_GLACIER_GB_MONTH
    )


def glue_cost_without_bookmarking(dpu_hours_per_run: float, runs_per_month: int) -> float:
    """Without bookmarking, every run reprocesses the full raw dataset."""
    return dpu_hours_per_run * runs_per_month * GLUE_DPU_HOUR


def glue_cost_with_bookmarking(dpu_hours_full_run: float, dpu_hours_incremental: float, runs_per_month: int) -> float:
    """First run of the month is a full scan; subsequent runs only process new data."""
    full_run_cost = dpu_hours_full_run * GLUE_DPU_HOUR
    incremental_cost = dpu_hours_incremental * (runs_per_month - 1) * GLUE_DPU_HOUR
    return full_run_cost + incremental_cost


def redshift_serverless_cost(rpu_hours_per_month: float) -> float:
    return rpu_hours_per_month * REDSHIFT_SERVERLESS_RPU_HOUR


def lambda_cost_with_batching(events_per_month: int, batch_size: int, avg_duration_ms: float, memory_mb: int) -> float:
    invocations = events_per_month / batch_size
    gb_seconds = invocations * (avg_duration_ms / 1000) * (memory_mb / 1024)
    return (invocations / 1_000_000) * LAMBDA_REQUEST_COST_PER_MILLION + gb_seconds * LAMBDA_GBSECOND_COST


def lambda_cost_without_batching(events_per_month: int, avg_duration_ms: float, memory_mb: int) -> float:
    """Naive baseline: one Lambda invocation per Kinesis record."""
    gb_seconds = events_per_month * (avg_duration_ms / 1000) * (memory_mb / 1024)
    return (events_per_month / 1_000_000) * LAMBDA_REQUEST_COST_PER_MILLION + gb_seconds * LAMBDA_GBSECOND_COST


def main():
    print("=" * 60)
    print("COST OPTIMIZATION IMPACT — AWS CUSTOMER 360 PLATFORM")
    print("=" * 60)

    # Storage — profile matching this platform's actual data volumes
    profile = StorageProfile(total_gb=1_800, pct_standard=0.25, pct_ia=0.35, pct_glacier=0.40)
    no_tier = s3_cost_without_tiering(profile)
    with_tier = s3_cost_with_tiering(profile)
    print(f"\nS3 Storage ({profile.total_gb:,.0f} GB):")
    print(f"  Without lifecycle tiering: ${no_tier:,.2f}/month")
    print(f"  With lifecycle tiering:    ${with_tier:,.2f}/month")
    print(f"  Monthly savings:           ${no_tier - with_tier:,.2f} ({(1 - with_tier/no_tier):.1%})")

    # Glue
    no_bookmark = glue_cost_without_bookmarking(dpu_hours_per_run=12, runs_per_month=30)
    with_bookmark = glue_cost_with_bookmarking(dpu_hours_full_run=12, dpu_hours_incremental=1.5, runs_per_month=30)
    print(f"\nGlue ETL (30 daily runs):")
    print(f"  Without job bookmarking:   ${no_bookmark:,.2f}/month")
    print(f"  With job bookmarking:      ${with_bookmark:,.2f}/month")
    print(f"  Monthly savings:           ${no_bookmark - with_bookmark:,.2f} ({(1 - with_bookmark/no_bookmark):.1%})")

    # Redshift Serverless vs always-on provisioned
    rpu_hours = 16 * 3  # base_capacity 16 RPU, ~3 active hours/day average usage
    serverless_cost = redshift_serverless_cost(rpu_hours * 30)
    print(f"\nRedshift Serverless (~{rpu_hours * 30:,.0f} RPU-hours/month vs always-on equivalent):")
    print(f"  Always-on provisioned (illustrative): ${REDSHIFT_PROVISIONED_EQUIVALENT_MONTHLY:,.2f}/month")
    print(f"  Serverless, pay-per-second:            ${serverless_cost:,.2f}/month")
    print(f"  Monthly savings:                       ${REDSHIFT_PROVISIONED_EQUIVALENT_MONTHLY - serverless_cost:,.2f} "
          f"({(1 - serverless_cost/REDSHIFT_PROVISIONED_EQUIVALENT_MONTHLY):.1%})")

    # Lambda batching
    events = 21_000_000  # ~700K events/day average across a month
    without_batch = lambda_cost_without_batching(events, avg_duration_ms=15, memory_mb=256)
    with_batch = lambda_cost_with_batching(events, batch_size=500, avg_duration_ms=180, memory_mb=256)
    print(f"\nLambda (Kinesis consumer, ~{events:,} events/month):")
    print(f"  Without batching (1 invoke/event): ${without_batch:,.2f}/month")
    print(f"  With batching (500/invoke):        ${with_batch:,.2f}/month")
    print(f"  Monthly savings:                   ${without_batch - with_batch:,.2f} ({(1 - with_batch/without_batch):.1%})")

    total_savings = (
        (no_tier - with_tier)
        + (no_bookmark - with_bookmark)
        + (REDSHIFT_PROVISIONED_EQUIVALENT_MONTHLY - serverless_cost)
        + (without_batch - with_batch)
    )
    print(f"\n{'TOTAL ESTIMATED MONTHLY SAVINGS:':<28} ${total_savings:,.2f}")
    print(f"{'TOTAL ESTIMATED ANNUAL SAVINGS:':<28} ${total_savings * 12:,.2f}")


if __name__ == "__main__":
    main()
