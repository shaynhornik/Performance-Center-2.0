#!/usr/bin/env python3
import os
import sys
import requests

TSHEETS_API_TOKEN = os.getenv("TSHEETS_API_TOKEN")
if not TSHEETS_API_TOKEN:
    print("❌ Please set TSHEETS_API_TOKEN in your environment")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {TSHEETS_API_TOKEN}",
    "Content-Type":  "application/json",
}

TSHEETS_BASE_URL = "https://rest.tsheets.com/api/v1"

def fetch_all_users():
    """
    Pulls all pages of /users from TSheets and returns a flat list of user dicts.
    """
    users = []
    page = 1
    while True:
        params = {"page": page, "per_page": 200}
        resp = requests.get(f"{TSHEETS_BASE_URL}/users", headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json().get("results", {}).get("users", {})
        batch = list(data.values())
        if not batch:
            break
        users.extend(batch)
        page += 1
    return users

if __name__ == "__main__":
    all_users = fetch_all_users()
    print(f"Fetched {len(all_users)} users\n")
    print(f"{'ID':<12} {'First Name':<15} {'Last Name':<15} {'Email'}")
    print("-" * 60)
    for u in all_users:
        print(f"{u['id']:<12} {u.get('first_name',''):<15} {u.get('last_name',''):<15} {u.get('email','<no email>')}")
