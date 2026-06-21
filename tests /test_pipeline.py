"""Run with: python -m unittest discover -s tests -v"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_quality"))

from dq_framework import check_completeness, check_uniqueness, check_referential_integrity, check_freshness


class TestDQFramework(unittest.TestCase):
    def test_completeness_passes_when_full(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        check = check_completeness(df, "test", ["a"], threshold=0.95)
        self.assertEqual(check.status, "PASS")

    def test_uniqueness_detects_duplicate_order_ids(self):
        df = pd.DataFrame({"order_id": ["O1", "O1", "O2"]})
        check = check_uniqueness(df, "transactions", "order_id", threshold=0.999)
        self.assertEqual(check.status, "FAIL")

    def test_referential_integrity_catches_orphan_customer(self):
        df = pd.DataFrame({"customer_id": ["C1", "C99"]})
        check = check_referential_integrity(df, "transactions", "customer_id", {"C1"}, threshold=0.99)
        self.assertEqual(check.status, "FAIL")


class TestPipelineLogic(unittest.TestCase):
    def test_zero_amount_transaction_rejected(self):
        df = pd.DataFrame({"amount": ["0", "150.00"], "quantity": ["2", "1"], "customer_id": ["C1", "C2"]})
        df["amount"] = pd.to_numeric(df["amount"])
        df["quantity"] = pd.to_numeric(df["quantity"])
        clean = df[(df["amount"] > 0) & (df["quantity"] > 0)]
        self.assertEqual(len(clean), 1)

    def test_anonymous_clickstream_filtered(self):
        df = pd.DataFrame({"customer_id": ["", "C1", None]})
        clean = df[df["customer_id"].notna() & (df["customer_id"] != "")]
        self.assertEqual(len(clean), 1)

    def test_conversion_rate_calculation(self):
        df = pd.DataFrame({"purchase": [10], "product_view": [100]})
        df["conversion_rate"] = (df["purchase"] / df["product_view"]).round(4)
        self.assertAlmostEqual(df.iloc[0]["conversion_rate"], 0.10)

    def test_conversion_rate_handles_zero_views(self):
        df = pd.DataFrame({"purchase": [0], "product_view": [0]})
        rate = (df["purchase"] / df["product_view"].replace(0, pd.NA)).fillna(0)
        self.assertEqual(rate.iloc[0], 0)


if __name__ == "__main__":
    unittest.main()
