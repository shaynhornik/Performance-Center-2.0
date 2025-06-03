import os, base64, requests

stmt = "a5f7c47a-8bac-43c7-bc90-7384fc74c171"   # <— your UUID

cid, sec = os.getenv("RAMP_CLIENT_ID"), os.getenv("RAMP_CLIENT_SECRET")
tok = requests.post(
    "https://api.ramp.com/developer/v1/token",
    headers={"Authorization":"Basic "+base64.b64encode(f"{cid}:{sec}".encode()).decode(),
             "Content-Type":"application/x-www-form-urlencoded"},
    data={"grant_type":"client_credentials",
          "scope":"business:read transactions:read statements:read"}
).json()["access_token"]

candidates = ["transactions", "statement_lines", "card_transactions"]
for res in candidates:
    url = f"https://api.ramp.com/developer/v1/statements/{stmt}/{res}?page_size=5"
    r   = requests.get(url, headers={"Authorization":f"Bearer {tok}",
                                     "Accept":"application/json"})
    print(f"{res:<18} → {r.status_code}")
    if r.status_code == 200:
        print("  sample ids:", [d.get("id") for d in r.json().get("data", [])][:3])

