"""
fetch_ramp_transactions.py
  • Retrieves *all* Ramp card‑transaction records (charges, refunds, disputes)
    between FROM_DATE and TO_DATE, even if >100 results.
  • Handles key‑set pagination using the `page.next` cursor.
  • Saves a simple CSV for inspection and returns the list of dicts.
"""

import os
import csv
import base64
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# ─── 0. Load env vars (if running locally with .env) ────────────────────────
load_dotenv()

CLIENT_ID     = os.getenv("RAMP_CLIENT_ID")
CLIENT_SECRET = os.getenv("RAMP_CLIENT_SECRET")
if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError("Set RAMP_CLIENT_ID and RAMP_CLIENT_SECRET in env")

# ─── 1. OAuth2 client‑credentials flow ───────────────────────────────────────
def get_access_token() -> str:
    auth   = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    token_resp = requests.post(
        "https://api.ramp.com/developer/v1/token",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type":  "application/x-www-form-urlencoded",
            "Accept":        "application/json",
        },
        data={
            "grant_type": "client_credentials",
            "scope":      "business:read transactions:read",
        },
        timeout=10,
    )
    token_resp.raise_for_status()
    return token_resp.json()["access_token"]

# ─── 2. Date window (edit as needed) ────────────────────────────────────────
# For a one‑time bootstrap grab everything since 2020‑01‑01:
# FROM_DATE = "2020-01-01T00:00:00Z"
# TO_DATE   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# For a demo, pull May 2025:
FROM_DATE = "2025-05-01T00:00:00Z"
TO_DATE   = "2025-05-31T23:59:59Z"

# ─── 3. Fetch all pages ─────────────────────────────────────────────────────
def fetch_all_transactions(from_dt: str, to_dt: str):
    token   = get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    base_url = "https://api.ramp.com/developer/v1/transactions"
    params   = {
        "limit": 100,
        "from_date": from_dt,
        "to_date":   to_dt,
        # You can add "state": "CLEARED" here if you only want settled spend.
    }

    all_txns, page_num = [], 0
    url = base_url                           # first call uses params dict

    while url:
        page_num += 1
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        txns      = payload.get("data", [])
        next_url  = payload.get("page", {}).get("next")
        all_txns.extend(txns)

        print(f"Page {page_num:>2}: fetched {len(txns)}  "
              f"(running total {len(all_txns)})")

        # After first call, we follow page.next URLs (no params)
        url, params = (next_url, None) if next_url else (None, None)

    print(f"Fetched {len(all_txns)} total transactions")
    return all_txns

# ─── 4. Flatten & write CSV for inspection ──────────────────────────────────
def write_csv(rows, path="transactions.csv"):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow([
            "id", "transaction_date", "state", "amount", "currency",
            "merchant_name", "card_holder", "has_receipt", "disputed"
        ])
        for tx in rows:
            w.writerow([
                tx["id"],
                tx.get("user_transaction_time"),
                tx.get("state"),
                tx.get("amount"),
                tx.get("currency_code", "USD"),
                tx.get("merchant_name"),
                (tx.get("card_holder") or {}).get("user_id"),
                bool(tx.get("receipts")),
                bool(tx.get("disputes")),
            ])
    print(f"Wrote CSV → {path}")

# ─── 5. Main entry ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    transactions = fetch_all_transactions(FROM_DATE, TO_DATE)
    write_csv(transactions)
