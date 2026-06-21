"""
Generates synthetic clickstream + e-commerce transaction data for the AWS
Customer 360 Platform — the domain used to demonstrate the full pipeline
locally before it's deployed as AWS Glue ETL jobs orchestrated by Step
Functions, with Kinesis handling the real-time clickstream leg.

Run: python engine/generate_sample_data.py
"""
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

rng = np.random.default_rng(99)
RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 50_000
N_PRODUCTS = 3_000
N_CLICKSTREAM_EVENTS = 700_000     # page views, add-to-cart, checkout events
N_TRANSACTIONS = 180_000            # completed orders

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"]
TRAFFIC_SOURCES = ["organic_search", "paid_search", "social", "email", "direct", "affiliate"]
CATEGORIES = ["Electronics", "Fashion", "Home", "Beauty", "Sports", "Books"]
ORDER_STATUSES = ["completed", "refunded", "cancelled"]

start = datetime(2025, 6, 1)
today = datetime(2026, 6, 1)

def _random_dates(n, start, end):
    delta_days = (end - start).days
    return [start + timedelta(days=int(o)) for o in rng.integers(0, delta_days, size=n)]

def _random_timestamps(n, start, end):
    delta_seconds = int((end - start).total_seconds())
    return [start + timedelta(seconds=int(o)) for o in rng.integers(0, delta_seconds, size=n)]

# ── Customers ────────────────────────────────────────────────────────────────
print("Generating customers...")
customer_ids = [f"CUST{str(i).zfill(7)}" for i in range(1, N_CUSTOMERS + 1)]
customers = pd.DataFrame({
    "customer_id": customer_ids,
    "signup_date": [d.strftime("%Y-%m-%d") for d in _random_dates(N_CUSTOMERS, start - timedelta(days=365), today)],
    "country": rng.choice(["ZA", "NG", "KE", "EG", "GH"], N_CUSTOMERS, p=[0.55, 0.20, 0.12, 0.08, 0.05]),
    "marketing_opt_in": rng.choice(["true", "false"], N_CUSTOMERS, p=[0.58, 0.42]),
})
customers.to_csv(RAW / "customers.csv", index=False)

# ── Products ─────────────────────────────────────────────────────────────────
print("Generating products...")
products = pd.DataFrame({
    "product_id": [f"PROD{str(i).zfill(6)}" for i in range(1, N_PRODUCTS + 1)],
    "category": rng.choice(CATEGORIES, N_PRODUCTS),
    "price": np.round(rng.gamma(2, 220, N_PRODUCTS) + 20, 2),
})
products.to_csv(RAW / "products.csv", index=False)

# ── Clickstream events (the big, real-time-style table) ─────────────────────
print(f"Generating {N_CLICKSTREAM_EVENTS:,} clickstream events...")
ck_customer = rng.choice(customer_ids + [""], N_CLICKSTREAM_EVENTS, p=[0.55 / N_CUSTOMERS] * N_CUSTOMERS + [0.45])
ck_session = rng.integers(100_000_000, 999_999_999, N_CLICKSTREAM_EVENTS)
ck_ts = _random_timestamps(N_CLICKSTREAM_EVENTS, start, today)
ck_event_type = rng.choice(EVENT_TYPES, N_CLICKSTREAM_EVENTS, p=[0.45, 0.25, 0.13, 0.05, 0.07, 0.05])
ck_product_idx = rng.integers(0, N_PRODUCTS, N_CLICKSTREAM_EVENTS)

clickstream = pd.DataFrame({
    "event_id": [f"EVT{str(i).zfill(9)}" for i in range(1, N_CLICKSTREAM_EVENTS + 1)],
    "customer_id": ck_customer,
    "session_id": [f"SESS{s}" for s in ck_session],
    "event_ts": [t.isoformat() for t in ck_ts],
    "event_type": ck_event_type,
    "product_id": np.array(products["product_id"])[ck_product_idx],
    "device_type": rng.choice(DEVICE_TYPES, N_CLICKSTREAM_EVENTS, p=[0.62, 0.30, 0.08]),
    "traffic_source": rng.choice(TRAFFIC_SOURCES, N_CLICKSTREAM_EVENTS),
})
clickstream.to_csv(RAW / "clickstream_events.csv", index=False)

# ── Transactions ─────────────────────────────────────────────────────────────
print(f"Generating {N_TRANSACTIONS:,} transactions...")
txn_customer = rng.choice(customer_ids, N_TRANSACTIONS)
txn_product_idx = rng.integers(0, N_PRODUCTS, N_TRANSACTIONS)
txn_dates = _random_dates(N_TRANSACTIONS, start, today)
txn_qty = rng.integers(1, 5, N_TRANSACTIONS)
unit_prices = np.array(products["price"])[txn_product_idx]
txn_amount = np.round(unit_prices * txn_qty, 2)

transactions = pd.DataFrame({
    "order_id": [f"ORD{str(i).zfill(8)}" for i in range(1, N_TRANSACTIONS + 1)],
    "customer_id": txn_customer,
    "product_id": np.array(products["product_id"])[txn_product_idx],
    "order_date": [d.strftime("%Y-%m-%d") for d in txn_dates],
    "quantity": txn_qty,
    "amount": txn_amount,
    "status": rng.choice(ORDER_STATUSES, N_TRANSACTIONS, p=[0.88, 0.07, 0.05]),
})
# Inject ~0.4% dirty records
n_dirty = int(N_TRANSACTIONS * 0.004)
dirty_idx = rng.choice(N_TRANSACTIONS, n_dirty, replace=False)
transactions.loc[dirty_idx[: n_dirty // 2], "quantity"] = -1
transactions.loc[dirty_idx[n_dirty // 2:], "amount"] = 0
transactions.to_csv(RAW / "transactions.csv", index=False)

total = len(customers) + len(products) + len(clickstream) + len(transactions)
print(f"\nDone. Total rows generated: {total:,}")
print(f"  customers: {len(customers):,} | products: {len(products):,}")
print(f"  clickstream_events: {len(clickstream):,} | transactions: {len(transactions):,}")
