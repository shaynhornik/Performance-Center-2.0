import os
import sys
import time
import requests
import asyncio
import aiohttp
from datetime import date, datetime, timedelta
from app.db import get_supabase

print("🔧 Loading ETL module as:", __name__, "– sys.argv:", sys.argv)

# ─── FLATTEN FUNCTIONS ────────────────────────────────────────────────────────

def flatten_employees(items):
    sb = get_supabase()
    rows = [{
        "id":            emp["id"],
        "email":         emp.get("email"),
        "first_name":    emp.get("first_name"),
        "last_name":     emp.get("last_name"),
        "role":          emp.get("role"),
        "company_id":    emp.get("company_id"),
        "avatar_url":    emp.get("avatar_url"),
        "color_hex":     emp.get("color_hex"),
        "mobile_number": emp.get("mobile_number"),
        "permissions":   emp.get("permissions"),
    } for emp in items]
    print(f"Flattening {len(rows)} employees → employees_flat")
    sb.table("employees_flat").upsert(rows).execute()


def flatten_customers(items):
    sb = get_supabase()
    rows = [{
        "id":                    cust["id"],
        "company_id":            cust.get("company_id"),
        "email":                 cust.get("email"),
        "first_name":            cust.get("first_name"),
        "last_name":             cust.get("last_name"),
        "created_at":            cust.get("created_at"),
        "updated_at":            cust.get("updated_at"),
        "home_number":           cust.get("home_number"),
        "work_number":           cust.get("work_number"),
        "mobile_number":         cust.get("mobile_number"),
        "lead_source":           cust.get("lead_source"),
        "notifications_enabled": cust.get("notifications_enabled"),
        "tags":                  cust.get("tags"),
        "company":               cust.get("company"),
        "addresses":             cust.get("addresses"),
        "notes":                 cust.get("notes"),
    } for cust in items]
    print(f"Flattening {len(rows)} customers → customers_flat")
    sb.table("customers_flat").upsert(rows).execute()


def flatten_estimates(items):
    sb = get_supabase()
    rows = []
    for est in items:
        est_id = est["id"]
        for tech in est.get("assigned_employees", []):
            tech_id = tech["id"]
            rows.append({
                "id":              f"{est_id}__{tech_id}",
                "estimate_id":     est_id,
                "technician_id":   tech_id,
                "created_at":      est.get("created_at"),
                "updated_at":      est.get("updated_at"),
                "work_status":     est.get("work_status"),
                "lead_source":     est.get("lead_source"),
                "company_id":      est.get("company_id"),
                "estimate_number": est.get("estimate_number"),
            })
    print(f"Flattening {len(rows)} estimate‑tech pairs → estimates_flat")
    sb.table("estimates_flat").upsert(rows).execute()


def flatten_jobs(items):
    sb = get_supabase()
    rows = []
    for job in items:
        job_id = job["id"]
        assigned = job.get("assigned_employees", [])
        count = len(assigned)
        completed_at = job.get("work_timestamps", {}).get("completed_at")
        for tech in assigned:
            tech_id = tech["id"]
            rows.append({
                "id":                  f"{job_id}__{tech_id}",
                "job_id":              job_id,
                "technician_id":       tech_id,
                "created_at":          job.get("created_at"),
                "updated_at":          job.get("updated_at"),
                "completed_at":        completed_at,
                "work_status":         job.get("work_status"),
                "total_amount":        job.get("total_amount"),
                "outstanding_balance": job.get("outstanding_balance"),
                "lead_source":         job.get("lead_source"),
                "company_id":          job.get("company_id"),
                "assigned_count":      count,
            })
    print(f"Flattening {len(rows)} job‑tech pairs → jobs_flat")
    sb.table("jobs_flat").upsert(rows).execute()


def flatten_job_appointments(items):
    sb = get_supabase()
    rows = []
    for appt in items:
        appt_id = appt["id"]
        for tech_id in appt.get("dispatched_employees_ids", []):
            rows.append({
                "id":                     f"{appt_id}__{tech_id}",
                "job_appointment_id":     appt_id,
                "dispatched_employee_id": tech_id,
                "start_time":             appt.get("start_time"),
                "end_time":               appt.get("end_time"),
                "arrival_window_minutes": appt.get("arrival_window_minutes"),
            })

    print(f"Flattening {len(rows)} appt‑tech pairs → job_appointments_flat")

    if rows:                                   # ← guard: only upsert if >0
        sb.table("job_appointments_flat").upsert(rows).execute()



def flatten_job_invoices(items):
    sb = get_supabase()
    rows = [{
        "id":             inv["id"],
        "amount":         inv.get("amount"),
        "status":         inv.get("status"),
        "due_at":         inv.get("due_at"),
        "paid_at":        inv.get("paid_at"),
        "sent_at":        inv.get("sent_at"),
        "invoice_date":   inv.get("invoice_date"),
        "service_date":   inv.get("service_date"),
        "due_amount":     inv.get("due_amount"),
        "invoice_number": inv.get("invoice_number"),
    } for inv in items]

    print(f"Flattening {len(rows)} invoices → job_invoices_flat")

    if rows:                                   # ← guard: only upsert if >0
        sb.table("job_invoices_flat").upsert(rows).execute()



def flatten_tsheets(items):
    sb = get_supabase()
    rows = []
    for ts in items:
        # turn empty‐string timestamps into NULLs
        start = ts.get("start") or None
        end   = ts.get("end")   or None
        lm    = ts.get("last_modified") or None

        # map TSheets user → HCP technician
        map_resp = (
            sb.table("tsheets_user_mapping")
              .select("technician_id")
              .eq("tsheets_user_id", ts["user_id"])
              .maybe_single()
              .execute()
        )
        tech_id = map_resp.data.get("technician_id") if map_resp and map_resp.data else None

        rows.append({
            "id":               ts["id"],
            "user_id":          ts["user_id"],
            "technician_id":    tech_id,
            "state":            ts["state"],
            "start_time":       start,
            "end_time":         end,
            "duration_seconds": ts["duration"],
            "entry_date":       ts["date"],
            "tz_offset":        ts["tz"],
            "tz_name":          ts["tz_str"],
            "entry_type":       ts["type"],
            "location":         ts["location"],
            "on_the_clock":     ts["on_the_clock"],
            "locked_in_days":   ts["locked"],
            "notes":            ts["notes"],
            "customfields":     ts["customfields"],
            "last_modified":    lm,
            "distance_tracking":   ts.get("distance_tracking", {}),
            "attached_files":      ts.get("attached_files", []),
            "created_by_user_id":  ts.get("created_by_user_id"),
        })
    print(f"Flattening {len(rows)} tsheets entries → tsheets_time_entries")
    sb.table("tsheets_time_entries").upsert(rows).execute()




# ─── INITIAL FLATTEN MAP ───────────────────────────────────────────────────
FLATTEN_MAP = {
    "employees":        flatten_employees,
    "customers":        flatten_customers,
    "estimates":        flatten_estimates,
    "jobs":             flatten_jobs,
    "job_appointments": flatten_job_appointments,
    "job_invoices":     flatten_job_invoices,
    "tsheets":          flatten_tsheets,
}

# ─── RAMP EXPENSES FETCHER & FLATTENER ────────────────────────────────
import scripts.fetch_ramp_expenses as ramp_fetcher

def fetch_ramp_expenses():
    """
    Return all Ramp transactions from the past 90 days.
    Adjust the window as needed (e.g. 30 days for hourly runs,
    or very wide for a one‑time bootstrap).
    """
    to_dt   = datetime.utcnow()
    from_dt = to_dt - timedelta(days=90)

    return ramp_fetcher.fetch_all_transactions(
        from_dt.isoformat(timespec="seconds") + "Z",
        to_dt  .isoformat(timespec="seconds") + "Z",
    )

def flatten_ramp_expenses(items):
    """Flatten the Ramp expense dicts into ramp_expenses_flat."""
    sb = get_supabase()
    rows = []
    for exp in items:
        rows.append({
            "id":                exp["id"],
            "transaction_time":  exp.get("user_transaction_time"),
            "amount":            exp.get("amount"),
            "currency_code":     exp.get("currency_code"),
            "merchant_name":     exp.get("merchant_name"),
            "spend_category":    exp.get("sk_category_name"),
            "status":            exp.get("state"),
            "receipt_count":     len(exp.get("receipts", [])),
            "synced_at":         exp.get("synced_at"),
        })
    print(f"Flattening {len(rows)} ramp expenses → ramp_expenses_flat")
    if rows:
        sb.table("ramp_expenses_flat").upsert(rows).execute()


FLATTEN_MAP["ramp_expenses"] = flatten_ramp_expenses
# ───────────────────────────────────────────────────────────────────────


# ─── TSheets USERS ──────────────────────────────────────────────────────────
def fetch_tsheets_users():
    url = f"{TSHEETS_BASE_URL}/users"
    print(f"GET {url}")
    resp = requests.get(url, headers=TSHEETS_HEADERS)
    resp.raise_for_status()
    users = resp.json().get("results", {}).get("users", {}) or {}
    return list(users.values())


def flatten_tsheets_users(items):
    sb = get_supabase()
    rows = [{
        "id":         u.get("id"),
        "first_name": u.get("first_name",""),
        "last_name":  u.get("last_name",""),
        "email":      u.get("email"),
    } for u in items]
    print(f"Flattening {len(rows)} TSheets users → tsheets_users_flat")
    sb.table("tsheets_users_flat").upsert(rows).execute()

# register users flattener
FLATTEN_MAP["tsheets_users"] = flatten_tsheets_users

# ─── HOUSECALL PRO FETCHERS ─────────────────────────────────────────────────
HCP_API_KEY    = os.environ["HCP_API_KEY"]
HCP_BASE_URL   = "https://api.housecallpro.com"
RATE_LIMIT_SEC = 0.4
HCP_HEADERS    = {"Authorization": f"Bearer {HCP_API_KEY}", "Content-Type": "application/json"}

def fetch_paginated(base_url, unwrap_key, page_size: int = 200):
    all_items, page = [], 1
    while True:
        url = f"{base_url}?page={page}&page_size={page_size}"
        print(f"GET {url}")
        res = requests.get(url, headers=HCP_HEADERS)
        if res.status_code != 200:
            print("❌", res.status_code, res.text)
            break
        batch = res.json().get(unwrap_key, [])
        if not batch:
            break
        all_items.extend(batch)
        page += 1
        time.sleep(RATE_LIMIT_SEC)
    print(f"Total {len(all_items)} {unwrap_key}")
    return all_items

async def _get_json(session, url):
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=HCP_HEADERS) as resp:
            if resp.status != 200:
                return {}
            return await resp.json()

async def _fetch_nested(job_ids, nested):
    """
    Fetch /jobs/<id>/<nested> for every job.
    `nested` is either 'job_appointments' or 'job_invoices'
    (legacy names used in ENTITY_CONFIG).

    New API paths & keys:
        job_appointments → appointments
        job_invoices     → invoices
    """
    path_key = {
        "job_appointments": "appointments",
        "job_invoices":     "invoices",
    }[nested]                         # will KeyError if we typo 'nested'

    base_url = f"{HCP_BASE_URL}/jobs"
    all_items = []

    async with aiohttp.ClientSession() as sess:
        tasks = [
            asyncio.create_task(
                _get_json(sess, f"{base_url}/{jid}/{path_key}")
            )
            for jid in job_ids
        ]

        for task in asyncio.as_completed(tasks):
            data = await task
            if not data:               # 404 or 204 ⇒ skip
                continue
            all_items.extend(data.get(path_key, []))

    return all_items



def fetch_employees():      return fetch_paginated(f"{HCP_BASE_URL}/employees", "employees")
def fetch_customers():      return fetch_paginated(f"{HCP_BASE_URL}/customers", "customers")
def fetch_estimates():      return fetch_paginated(f"{HCP_BASE_URL}/estimates", "estimates")
def fetch_jobs():           return fetch_paginated(f"{HCP_BASE_URL}/jobs",      "jobs")
def fetch_job_appointments(): return asyncio.run(_fetch_nested(_job_ids(), "job_appointments"))
def fetch_job_invoices():     return asyncio.run(_fetch_nested(_job_ids(), "job_invoices"))

def _job_ids():
    sb, ids, step, start = get_supabase(), [], 1000, 0
    while True:
        res = sb.table("hcp_raw_jobs").select("id").range(start, start+step-1).execute()
        batch = [r["id"] for r in res.data or []]
        ids.extend(batch)
        if len(batch) < step:
            break
        start += step
    return ids

# ─── TSHEETS FETCHERS ────────────────────────────────────────────────────────
TSHEETS_BASE_URL   = "https://rest.tsheets.com/api/v1"
TSHEETS_API_TOKEN  = os.getenv("TSHEETS_API_TOKEN") or sys.exit("Missing TSHEETS_API_TOKEN")
TSHEETS_HEADERS    = {"Authorization": f"Bearer {TSHEETS_API_TOKEN}", "Content-Type": "application/json"}

def fetch_tsheets_timesheets(start_date: str, end_date: str, page: int = 1):
    params = {"start_date": start_date, "end_date": end_date, "limit": 200, "page": page}
    url = f"{TSHEETS_BASE_URL}/timesheets"
    print(f"GET {url} with {params}")
    resp = requests.get(url, headers=TSHEETS_HEADERS, params=params)
    resp.raise_for_status()
    return list(resp.json().get("results", {}).get("timesheets", {}).values())

def fetch_all_tsheets_timesheets(start_date: str, end_date: str):
    all_items, page = [], 1
    while True:
        batch = fetch_tsheets_timesheets(start_date, end_date, page)
        if not batch:
            break
        all_items.extend(batch)
        page += 1
    return all_items

# ─── ENTITY CONFIG ───────────────────────────────────────────────────────────
ENTITY_CONFIG = {
    "employees":        {"fetch": fetch_employees, "table": "hcp_raw_employees",       "id": lambda itm: itm["id"]},
    "customers":        {"fetch": fetch_customers, "table": "hcp_raw_customers",       "id": lambda itm: itm["id"]},
    "estimates":        {"fetch": fetch_estimates, "table": "hcp_raw_estimates",      "id": lambda itm: itm["id"]},
    "jobs":             {"fetch": fetch_jobs,      "table": "hcp_raw_jobs",            "id": lambda itm: itm["id"]},
    "job_appointments": {"fetch": fetch_job_appointments, "table": "hcp_raw_job_appointments", "id": lambda itm: itm["id"]},
    "job_invoices":     {"fetch": fetch_job_invoices,    "table": "hcp_raw_job_invoices",     "id": lambda itm: itm["id"]},
    "tsheets":          {"fetch": lambda: fetch_all_tsheets_timesheets("2022-06-12", date.today().isoformat()),
                             "table": "tsheets_time_entries",   
                             "id":    lambda itm: itm["id"]},
    "tsheets_users":    {"fetch": fetch_tsheets_users,        "table": "tsheets_users_flat",
                             "id":    lambda u: u["id"]},
    "ramp_expenses": {
        "fetch": fetch_ramp_expenses,
        "table": "ramp_raw_expenses",
        "id":    lambda exp: exp["id"],
    },
    "bootstrap":        {"fetch": lambda: None,            "table": None,                   "id": None},
}

# ─── UPSERT & CLI ENTRY ───────────────────────────────────────────────────────

# ─── REPLACE your current upsert() with this version ──────────────
def upsert(entity: str, items):
    """
    Push raw payloads into the *_raw_* table for <entity>.
    Prints exactly one concise line: the row count + table name.
    """
    cfg = ENTITY_CONFIG[entity]
    sb  = get_supabase()
    rows = [{"id": cfg["id"](itm), "payload": itm} for itm in items]

    if rows:
        print(f"   ↳ upserting {len(rows):>4} rows → {cfg['table']}")
        sb.table(cfg['table']).upsert(rows).execute()
# ──────────────────────────────────────────────────────────────────



def main():
    print("🛠️  ETL starting with args:", sys.argv)
    if len(sys.argv) < 2 or sys.argv[1] not in ENTITY_CONFIG:
        print("Usage: python3 -m app.etl <entity>")
        print("Entities:", ", ".join(ENTITY_CONFIG))
        return
    entity = sys.argv[1]

    # Special-case raw-less flatteners
    if entity in ("tsheets", "tsheets_users"):
        items = ENTITY_CONFIG[entity]["fetch"]()
        FLATTEN_MAP[entity](items)
        return

    # Bootstrap placeholder
    if entity == "bootstrap":
        print("Use dedicated bootstrap function if needed.")
        return

    items = ENTITY_CONFIG[entity]["fetch"]()
    upsert(entity, items)
    if entity in FLATTEN_MAP:
        FLATTEN_MAP[entity](items)

# ─── CONVENIENCE WRAPPER FOR SUPABASE WEBHOOK ─────────────────────
def run_full_sync():
    """
    Fetches **everything** we care about and writes it into Supabase.
    Called by the /cron/hourly webhook so pg_cron doesn't need
    to pass entity names one by one.
    """
    entities = [
        "employees", "customers", "estimates",
        "jobs", "job_appointments", "job_invoices",
        "tsheets", "ramp_expenses"
    ]

    for entity in entities:
        print(f"🔄 Syncing {entity} …")

        # special‑case TSheets and Ramp because their fetchers differ
        if entity == "tsheets":
            items = ENTITY_CONFIG["tsheets"]["fetch"]()
            FLATTEN_MAP["tsheets"](items)
            continue

        if entity == "ramp_expenses":
            items = fetch_ramp_expenses()          # ← uses your no‑arg wrapper
            upsert("ramp_expenses", items)
            FLATTEN_MAP["ramp_expenses"](items)
            continue

        # default path for Housecall Pro entities
        items = ENTITY_CONFIG[entity]["fetch"]()
        upsert(entity, items)
        if entity in FLATTEN_MAP:
            FLATTEN_MAP[entity](items)

    print("✅ Full ETL cycle completed")


if __name__ == "__main__":
    main()





