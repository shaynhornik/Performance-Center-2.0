import os, sys, time, requests, asyncio, aiohttp
from app.db import get_supabase

import sys
print("🔧 Loading ETL module as:", __name__, "– sys.argv:", sys.argv)


# ─── FLATTEN FUNCTIONS ────────────────────────────────────────────────────────

def flatten_employees(items):
    """Take raw employee payloads and upsert into employees_flat."""
    sb = get_supabase()
    rows = []
    for emp in items:
        # emp is the raw JSON payload
        rows.append({
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
        })
    print(f"Flattening {len(rows)} employees → employees_flat")
    sb.table("employees_flat").upsert(rows).execute()


def flatten_customers(items):
    """Flatten raw customer payloads into customers_flat."""
    sb = get_supabase()
    rows = []
    for cust in items:
        rows.append({
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
        })
    print(f"Flattening {len(rows)} customers → customers_flat")
    sb.table("customers_flat").upsert(rows).execute()


def flatten_estimates(items):
    """One row per (estimate × technician) into estimates_flat."""
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
    print(f"Flattening {len(rows)} estimate‐tech pairs → estimates_flat")
    sb.table("estimates_flat").upsert(rows).execute()


def flatten_jobs(items):
    """One row per (job × technician) into jobs_flat."""
    sb = get_supabase()
    rows = []
    for job in items:
        job_id = job["id"]
        for tech in job.get("assigned_employees", []):
            tech_id = tech["id"]
            rows.append({
                "id":                   f"{job_id}__{tech_id}",
                "job_id":               job_id,
                "technician_id":        tech_id,
                "created_at":           job.get("created_at"),
                "updated_at":           job.get("updated_at"),
                "work_status":          job.get("work_status"),
                "total_amount":         job.get("total_amount"),
                "outstanding_balance":  job.get("outstanding_balance"),
                "lead_source":          job.get("lead_source"),
                "company_id":           job.get("company_id"),
            })
    print(f"Flattening {len(rows)} job‐tech pairs → jobs_flat")
    sb.table("jobs_flat").upsert(rows).execute()


def flatten_job_appointments(items):
    """One row per (appointment × dispatched_employee) into job_appointments_flat."""
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
    print(f"Flattening {len(rows)} appt‐tech pairs → job_appointments_flat")
    sb.table("job_appointments_flat").upsert(rows).execute()


def flatten_job_invoices(items):
    """Flatten each invoice payload into job_invoices_flat."""
    sb = get_supabase()
    rows = []
    for inv in items:
        rows.append({
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
        })
    print(f"Flattening {len(rows)} invoices → job_invoices_flat")
    sb.table("job_invoices_flat").upsert(rows).execute()


# Map each raw‐table entity to its flatten function
FLATTEN_MAP = {
    "employees":           flatten_employees,
    "customers":           flatten_customers,
    "estimates":           flatten_estimates,
    "jobs":                flatten_jobs,
    "job_appointments":    flatten_job_appointments,
    "job_invoices":        flatten_job_invoices,
}


# ------------------------------------------------------------------
# Async parallel fetchers for job_appointments & job_invoices
# ------------------------------------------------------------------

MAX_REQ_PER_MIN = 150
SEM = asyncio.Semaphore(MAX_REQ_PER_MIN)

async def _get_json(session, url):
    async with SEM:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 404:
                return []
            if resp.status == 400:
                txt = await resp.text()
                if "Archived job" in txt:
                    return []        # just skip
            if resp.status != 200:
                print("❌", resp.status, await resp.text())
                return []
            return await resp.json()

async def _fetch_nested(job_ids, nested):
    """
    nested = "appointments"  OR  "invoices"
    Endpoint: /jobs/{id}/{nested}
    """
    key  = nested
    base = f"{HCP_BASE_URL}/jobs"
    all_items = []

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(_get_json(session, f"{base}/{jid}/{nested}"))
            for jid in job_ids
        ]
        for task in asyncio.as_completed(tasks):
            data = await task
            if data:
                all_items.extend(data.get(key, []))
    return all_items

def fetch_job_appointments():
    job_ids = _job_ids()
    print(f"Fetching appointments for {len(job_ids)} jobs …")
    return asyncio.run(_fetch_nested(job_ids, "appointments"))

def fetch_job_invoices():
    job_ids = _job_ids()
    print(f"Fetching invoices for {len(job_ids)} jobs …")
    return asyncio.run(_fetch_nested(job_ids, "invoices"))


from app.db import get_supabase

HCP_API_KEY = os.environ["HCP_API_KEY"]
HCP_BASE_URL = "https://api.housecallpro.com"
RATE_LIMIT_SECONDS = 0.4

headers = {
    "Authorization": f"Bearer {HCP_API_KEY}",
    "Content-Type": "application/json",
}

def fetch_paginated(base_url, unwrap_key, page_size: int = 200):
    """
    Generic paginator that keeps requesting ?page=N&page_size=page_size
    until the API returns an empty list.
    """
    all_items, page = [], 1
    while True:
        url = f"{base_url}?page={page}&page_size={page_size}"
        print(f"GET {url}")
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print("❌", res.status_code, res.text)
            break

        data   = res.json()
        batch  = data.get(unwrap_key, [])
        count  = len(batch)
        print(f"  → received {count}")

        if count == 0:
            break

        all_items.extend(batch)
        page += 1
        time.sleep(RATE_LIMIT_SECONDS)

    print(f"Total {len(all_items)} items")
    return all_items


# ------------------------------------------------------------------
# Top‑level entities (one call per page)
# ------------------------------------------------------------------
def fetch_employees():
    return fetch_paginated(f"{HCP_BASE_URL}/employees", "employees")

def fetch_customers():
    return fetch_paginated(f"{HCP_BASE_URL}/customers", "customers")

def fetch_estimates():
    return fetch_paginated(f"{HCP_BASE_URL}/estimates", "estimates")

def fetch_jobs():
    return fetch_paginated(f"{HCP_BASE_URL}/jobs", "jobs")

# ------------------------------------------------------------------
# Nested entities – require iterating over jobs
# ------------------------------------------------------------------
def _job_ids():
    """
    Return *all* job IDs from Supabase, paging in blocks of 1 000
    to bypass the PostgREST hard limit.
    """
    sb   = get_supabase()
    ids  = []
    step = 1000
    start = 0

    while True:
        res = (
            sb.table("hcp_raw_jobs")
              .select("id")
              .range(start, start + step - 1)   # 0‑based, inclusive
              .execute()
        )
        batch = [row["id"] for row in res.data]
        ids.extend(batch)

        if len(batch) < step:     # last page
            break

        start += step

    print(f"Collected {len(ids)} job IDs from Supabase")
    return ids





# ------------------------------------------------------------------
# 🚀  BOOTSTRAP — pull jobs *with* nested appointments & invoices
# ------------------------------------------------------------------
def bootstrap_jobs_with_nested(page_size: int = 200):
    """
    Runs one paginated pass of /jobs?include=appointments,invoices
    and fans the nested arrays into their own raw tables.
    """
    print("🚀  BOOTSTRAP: /jobs?include=appointments,invoices")
    sb = get_supabase()

    page = 1
    total_jobs = total_appts = total_invoices = 0

    while True:
        url = (
            f"{HCP_BASE_URL}/jobs"
            f"?include=job_appointments,job_invoices"
            f"&page={page}&page_size={page_size}"
        )
        print(f"GET {url}")
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print("❌", res.status_code, res.text)
            break

        data  = res.json()
        jobs  = data.get("jobs", [])
        if not jobs:
            break   # no more pages

        # ---- split into three lists ---------------------------------
        job_rows, appt_rows, inv_rows = [], [], []

        for j in jobs:
            job_rows.append({"id": j["id"], "payload": j})

            for a in j.get("job_appointments", []):
                appt_rows.append({"id": a["id"], "payload": a})

            for i in j.get("job_invoices", []):
                inv_rows.append({"id": i["id"], "payload": i})

        # ---- upsert each batch --------------------------------------
        if job_rows:
            sb.table("hcp_raw_jobs").upsert(job_rows).execute()
        if appt_rows:
            sb.table("hcp_raw_job_appointments").upsert(appt_rows).execute()
        if inv_rows:
            sb.table("hcp_raw_job_invoices").upsert(inv_rows).execute()

        # stats
        total_jobs      += len(job_rows)
        total_appts     += len(appt_rows)
        total_invoices  += len(inv_rows)
        print(
            f"  → jobs:{len(job_rows)}  appts:{len(appt_rows)}"
            f"  invoices:{len(inv_rows)}"
        )

        page += 1
        time.sleep(RATE_LIMIT_SECONDS)  # respect 150 req/min

    print(
        f"✅  BOOTSTRAP complete — "
        f"jobs:{total_jobs}  appts:{total_appts}  invoices:{total_invoices}"
    )


# ------------------------------------------------------------------
# Config map
# ------------------------------------------------------------------
ENTITY_CONFIG = {
    "employees": {
        "fetch": fetch_employees,
        "table": "hcp_raw_employees",
        "id": lambda itm: itm["id"],
    },
    "customers": {
        "fetch": fetch_customers,
        "table": "hcp_raw_customers",
        "id": lambda itm: itm["id"],
    },
    "estimates": {
        "fetch": fetch_estimates,
        "table": "hcp_raw_estimates",
        "id": lambda itm: itm["id"],
    },
    "jobs": {
        "fetch": fetch_jobs,
        "table": "hcp_raw_jobs",
        "id": lambda itm: itm["id"],
    },
    "job_appointments": {
        "fetch": fetch_job_appointments,
        "table": "hcp_raw_job_appointments",
        "id": lambda itm: itm["id"],
    },
    "job_invoices": {
        "fetch": fetch_job_invoices,
        "table": "hcp_raw_job_invoices",
        "id": lambda itm: itm["id"],
    },
    "bootstrap": {            # one‑time bulk import
        "fetch": bootstrap_jobs_with_nested,
        "table": None,
        "id":   None,
    },
}
# ------------------------------------------------------------------
# Shared upsert & CLI entry
# ------------------------------------------------------------------
def upsert(entity: str, items):
    cfg = ENTITY_CONFIG[entity]
    sb  = get_supabase()
    rows = []

    for itm in items[:3]:                 # 👈 preview first 3
        print("SAMPLE item:", itm)        # 👈 NEW

    for itm in items:
        try:
            rows.append({"id": cfg["id"](itm), "payload": itm})
        except Exception as e:
            print("Skipping item:", e)

    print(f"Built {len(rows)} rows")      # 👈 NEW

    if not rows:
        print("No rows to insert.")
        return

    print(f"Upserting {len(rows)} into {cfg['table']} …")
    resp = sb.table(cfg["table"]).upsert(rows).execute()
    print("Supabase response:", resp)     # 👈 NEW
    print("✅ Done.")


def main():
     print("🛠️  ETL starting with args:", sys.argv)
     if len(sys.argv) < 2 or sys.argv[1] not in ENTITY_CONFIG:
         print("Usage: python3 -m app.etl <entity>")
         print("Entities:", ", ".join(ENTITY_CONFIG))
         return
     entity = sys.argv[1]
 
     # 1-time bootstrap
     if entity == "bootstrap":
         ENTITY_CONFIG["bootstrap"]["fetch"]()
         return
 
     # fetch raw & upsert
     items = ENTITY_CONFIG[entity]["fetch"]()
     upsert(entity, items)
 
     # NOW flatten into your *_flat tables
     if entity in FLATTEN_MAP:
         FLATTEN_MAP[entity](items)


if __name__ == "__main__":
    main()
