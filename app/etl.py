import os, sys, time, requests, asyncio, aiohttp
from app.db import get_supabase

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
    if len(sys.argv) < 2 or sys.argv[1] not in ENTITY_CONFIG:
        print("Usage: python3 -m app.etl <entity>")
        print("Entities:", ", ".join(ENTITY_CONFIG))
        return
    entity = sys.argv[1]
    # Special one‑time command
    if entity == "bootstrap":
        ENTITY_CONFIG["bootstrap"]["fetch"]()
        return

    items = ENTITY_CONFIG[entity]["fetch"]()
    upsert(entity, items)

if __name__ == "__main__":
    main()
