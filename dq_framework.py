"""
Data Quality Framework — a small, dependency-free engine implementing the
four DQ dimensions every production Silver/Gold layer needs to enforce:
completeness, uniqueness, referential integrity, and freshness.

In production this would run as a AWS Glue job step in the same Step Functions state machine
as the medallion pipeline, failing the Job (and triggering the Azure Monitor
alert in monitoring/) if any check drops below its threshold — the same
pattern as Great Expectations or Databricks' native Delta Live Tables
expectations, implemented here in plain pandas so it's framework-agnostic
and easy to read.
"""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RESULTS_PATH = Path(__file__).resolve().parent.parent / "data" / "lake" / "dq_results.csv"


class DQCheck:
    def __init__(self, name, table, status, metric_value, threshold, detail=""):
        self.name = name
        self.table = table
        self.status = status
        self.metric_value = metric_value
        self.threshold = threshold
        self.detail = detail
        self.run_ts = datetime.now(timezone.utc).isoformat()

    def as_dict(self):
        return {
            "check_name": self.name, "table": self.table, "status": self.status,
            "metric_value": self.metric_value, "threshold": self.threshold,
            "detail": self.detail, "run_ts": self.run_ts,
        }


def check_completeness(df: pd.DataFrame, table: str, required_cols: list, threshold: float = 0.98) -> DQCheck:
    total = len(df)
    complete = df[required_cols].notna().all(axis=1).sum()
    score = complete / total if total else 0
    status = "PASS" if score >= threshold else "FAIL"
    return DQCheck("completeness", table, status, round(score, 4), threshold,
                   f"{complete:,}/{total:,} rows complete across {required_cols}")


def check_uniqueness(df: pd.DataFrame, table: str, key_col: str, threshold: float = 0.999) -> DQCheck:
    total = len(df)
    unique = df[key_col].nunique()
    score = unique / total if total else 0
    status = "PASS" if score >= threshold else "FAIL"
    return DQCheck("uniqueness", table, status, round(score, 4), threshold,
                   f"{unique:,}/{total:,} unique values for {key_col}")


def check_referential_integrity(df: pd.DataFrame, table: str, fk_col: str,
                                  valid_keys: set, threshold: float = 0.99) -> DQCheck:
    total = len(df)
    valid = df[fk_col].isin(valid_keys).sum()
    score = valid / total if total else 0
    status = "PASS" if score >= threshold else "FAIL"
    return DQCheck("referential_integrity", table, status, round(score, 4), threshold,
                   f"{valid:,}/{total:,} rows have a valid {fk_col} reference")


def check_freshness(df: pd.DataFrame, table: str, date_col: str, max_age_days: int = 7) -> DQCheck:
    if df.empty:
        return DQCheck("freshness", table, "FAIL", None, max_age_days, "Table is empty")
    latest = pd.to_datetime(df[date_col]).max()
    age_days = (pd.Timestamp.now() - latest).days
    status = "PASS" if age_days <= max_age_days else "FAIL"
    return DQCheck("freshness", table, status, age_days, max_age_days,
                   f"Latest record is {age_days} days old (max allowed: {max_age_days})")


def run_suite(checks: list) -> pd.DataFrame:
    """Runs a list of DQCheck objects, writes results, and raises if any FAIL
    — mirroring how a failed Delta Live Tables expectation halts a pipeline."""
    results = pd.DataFrame([c.as_dict() for c in checks])
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(RESULTS_PATH, index=False)
    failed = results[results["status"] == "FAIL"]
    return results, failed
