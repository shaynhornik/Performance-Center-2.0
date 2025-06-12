# ──────────────────────────────────────────────────────────────────────────────
# app/etl.py  –  fully-async ETL for HouseCall Pro, TSheets & Ramp
# ──────────────────────────────────────────────────────────────────────────────
"""
• Pulls paginated endpoints concurrently (async) with progress bars
• Fetches job-level “nested” resources (appointments / invoices) in parallel
• Writes raw ➜ flat tables in Supabase
Environment vars required
-------------------------
HCP_API_KEY, TSHEETS_API_TOKEN, SUPABASE_URL, SUPABASE_ANON_KEY, …
"""
from __future__ import annotations

import asyncio
import math
import os
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List

import aiohttp
import requests
from aiohttp import ClientTimeout
from postgrest.exceptions import APIError                # noqa: F401  (imported elsewhere)
from tqdm.asyncio import tqdm

from app.db import get_supabase
import scripts.fetch_ramp_expenses as ramp_fetcher       # Ramp helper

print("🔧 Loading ETL module as:", __name__, "– sys.argv:", sys.argv)

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
HCP_API_KEY  = os.getenv("HCP_API_KEY")  or sys.exit("Missing HCP_API_KEY")
HCP_BASE_URL = "https://api.housecallpro.com"
HCP_HEADERS  = {"Authorization": f"Bearer {HCP_API_KEY}", "Content-Type": "application/json"}

TSHEETS_API_TOKEN = os.getenv("TSHEETS_API_TOKEN") or sys.exit("Missing TSHEETS_API_TOKEN")
TSHEETS_BASE_URL  = "https://rest.tsheets.com/api/v1"
TSHEETS_HEADERS   = {"Authorization": f"Bearer {TSHEETS_API_TOKEN}", "Content-Type": "application/json"}

# ──────────────────────────────────────────────────────────────────────────────
# Generic async helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _get_json(session: aiohttp.ClientSession, url: str, *, sem: asyncio.Semaphore, delay: float = 0.0) -> dict:
    """Internal: GET + JSON with semaphore / delay (honours rate-limit)."""
    async with sem:
        if delay:
            await asyncio.sleep(delay)
        async with session.get(url, headers=HCP_HEADERS, timeout=ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"{url} -> {resp.status}")
            return await resp.json()


async def _fetch_page(session: aiohttp.ClientSession, url: str, unwrap_key: str, *, sem: asyncio.Semaphore, delay: float) -> list[dict]:
    try:
        data = await _get_json(session, url, sem=sem, delay=delay)
        return data.get(unwrap_key, [])
    except Exception as exc:  # noqa: BLE001
        print(f"❌ {url} -> {exc}")
        return []


async def fetch_paginated_async(
    base_url: str,
    unwrap_key: str,
    *,
    page_size: int = 200,
    max_concurrency: int = 8,
    delay_per_req: float = 0.15,
) -> list[dict]:
    """
    Fetch *all* pages for a “simple” HCP endpoint concurrently.
    Stops when a page returns < `page_size` rows.
    """
    rows: list[dict] = []
    sem  = asyncio.Semaphore(max_concurrency)

    async with aiohttp.ClientSession() as session:
        page  = 1
        done  = False
        pbar  = tqdm(total=0, unit="pg", desc=f"Fetching {unwrap_key}")

        while not done:
            pages = range(page, page + max_concurrency)
            tasks = [
                asyncio.create_task(
                    _fetch_page(session, f"{base_url}?page={pg}&page_size={page_size}", unwrap_key, sem=sem, delay=delay_per_req)
                )
                for pg in pages
            ]
            batches = await asyncio.gather(*tasks)
            flat    = [r for batch in batches for r in batch]
            rows.extend(flat)

            page += max_concurrency
            pbar.update(len(batches))

            # if ANY of those pages had < full page_size ⇒ reached the tail
            if any(len(batch) < page_size for batch in batches):
                done = True

        pbar.close()

    print(f"Total {len(rows)} {unwrap_key} (fetched async, all pages)")
    return rows


async def _fetch_nested_async(
    job_ids: list[str],
    nested: str,
    *,
    max_concurrency: int = 40,
    delay_per_req: float = 0.02,
) -> list[dict]:
    """Fetch /jobs/<id>/<appointments|invoices> for every job ID concurrently."""
    path_key = {"job_appointments": "appointments", "job_invoices": "invoices"}[nested]
    sem      = asyncio.Semaphore(max_concurrency)
    rows: list[dict] = []

    async with aiohttp.ClientSession() as session:
        async def _task(jid: str) -> list[dict]:
            url = f"{HCP_BASE_URL}/jobs/{jid}/{path_key}"
            try:
                data = await _get_json(session, url, sem=sem, delay=delay_per_req)
                return data.get(path_key, [])
            except Exception:
                return []         # 400s are expected for some jobs

        tasks = [asyncio.create_task(_task(j)) for j in job_ids]

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="job", desc=f"Fetching {nested}"):
            rows.extend(await coro)

    print(f"Total {len(rows)} {nested} (fetched async)")
    return rows

# ─── async helpers for TSheets ──────────────────────────────────────────────
async def _get_json_tsheets(
    session: aiohttp.ClientSession,
    url: str,
    *,
    sem: asyncio.Semaphore,
    delay: float = 0.0,
) -> dict:
    async with sem:
        if delay:
            await asyncio.sleep(delay)
        async with session.get(url, headers=TSHEETS_HEADERS, timeout=ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"{url} -> {resp.status}")
            return await resp.json()


async def _fetch_tsheets_page(
    session: aiohttp.ClientSession,
    url: str,
    *,
    sem: asyncio.Semaphore,
    delay: float,
) -> list[dict]:
    try:
        data = await _get_json_tsheets(session, url, sem=sem, delay=delay)
        return list(data.get("results", {}).get("timesheets", {}).values())
    except Exception as exc:   # noqa: BLE001
        print(f"❌ {url} -> {exc}")
        return []


async def fetch_tsheets_async(
    start_date: str,
    end_date: str,
    *,
    limit: int = 200,
    max_concurrency: int = 8,
    delay_per_req: float = 0.15,
) -> list[dict]:
    """Fully-async paginator for /timesheets."""
    rows: list[dict] = []
    sem = asyncio.Semaphore(max_concurrency)

    base = f"{TSHEETS_BASE_URL}/timesheets?start_date={start_date}&end_date={end_date}&limit={limit}"
    page = 1
    done = False
    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=0, unit="pg", desc="Fetching tsheets")
        while not done:
            tasks = [
                asyncio.create_task(
                    _fetch_tsheets_page(
                        session,
                        f"{base}&page={pg}",
                        sem=sem,
                        delay=delay_per_req,
                    )
                )
                for pg in range(page, page + max_concurrency)
            ]
            batches = await asyncio.gather(*tasks)
            flat = [r for batch in batches for r in batch]
            rows.extend(flat)

            page += max_concurrency
            pbar.update(len(batches))

            # if any page is short / empty → we reached the end
            if any(len(batch) < limit for batch in batches):
                done = True
        pbar.close()

    print(f"Total {len(rows)} tsheets (fetched async, all pages)")
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# WRAPPER fetchers – connect helpers to registry
# ──────────────────────────────────────────────────────────────────────────────
fetch_employees = lambda: fetch_paginated_async(f"{HCP_BASE_URL}/employees", "employees")
fetch_customers = lambda: fetch_paginated_async(f"{HCP_BASE_URL}/customers", "customers")
fetch_estimates = lambda: fetch_paginated_async(f"{HCP_BASE_URL}/estimates", "estimates")
fetch_jobs      = lambda: fetch_paginated_async(f"{HCP_BASE_URL}/jobs",      "jobs")


def _get_job_ids(batch: int = 1000) -> list[str]:
    """Return *all* job IDs that already exist in hcp_raw_jobs."""
    sb, start, ids = get_supabase(), 0, []
    while True:
        resp  = sb.table("hcp_raw_jobs").select("id").range(start, start + batch - 1).execute()
        chunk = [r["id"] for r in (resp.data or [])]
        ids.extend(chunk)
        if len(chunk) < batch:
            break
        start += batch
    return ids


def fetch_job_appointments():
    return _fetch_nested_async(_get_job_ids(), "job_appointments")


def fetch_job_invoices():
    return _fetch_nested_async(_get_job_ids(), "job_invoices")

# ──────────────────────────────────────────────────────────────────────────────
#  … EVERYTHING BELOW THIS LINE IS IDENTICAL TO THE VERSION YOU POSTED …
#  (flatteners, Ramp, T-Sheets hel


# ──────────────────────────────────────────────────────────────────────────────
# Flatten helpers (UNCHANGED from your previous file)
# ──────────────────────────────────────────────────────────────────────────────
def flatten_employees(items):
    sb = get_supabase()
    rows = [
        {
            "id": emp.get("id"),
            "email": emp.get("email"),
            "first_name": emp.get("first_name"),
            "last_name": emp.get("last_name"),
            "role": emp.get("role"),
            "company_id": emp.get("company_id"),
            "avatar_url": emp.get("avatar_url"),
            "color_hex": emp.get("color_hex"),
            "mobile_number": emp.get("mobile_number"),
            "permissions": emp.get("permissions"),
        }
        for emp in items
    ]
    print(f"Flattening {len(rows)} employees → employees_flat")
    sb.table("employees_flat").upsert(rows).execute()


def flatten_customers(items):
    sb = get_supabase()
    rows = [
        {
            "id": cust.get("id"),
            "company_id": cust.get("company_id"),
            "email": cust.get("email"),
            "first_name": cust.get("first_name"),
            "last_name": cust.get("last_name"),
            "created_at": cust.get("created_at"),
            "updated_at": cust.get("updated_at"),
            "home_number": cust.get("home_number"),
            "work_number": cust.get("work_number"),
            "mobile_number": cust.get("mobile_number"),
            "lead_source": cust.get("lead_source"),
            "notifications_enabled": cust.get("notifications_enabled"),
            "tags": cust.get("tags"),
            "company": cust.get("company"),
            "addresses": cust.get("addresses"),
            "notes": cust.get("notes"),
        }
        for cust in items
    ]
    print(f"Flattening {len(rows)} customers → customers_flat")
    sb.table("customers_flat").upsert(rows).execute()


def flatten_estimates(items):
    sb = get_supabase()
    rows = []
    for est in items:
        est_id = est.get("id")
        for tech in est.get("assigned_employees", []):
            tech_id = tech.get("id")
            rows.append(
                {
                    "id": f"{est_id}__{tech_id}",
                    "estimate_id": est_id,
                    "technician_id": tech_id,
                    "created_at": est.get("created_at"),
                    "updated_at": est.get("updated_at"),
                    "work_status": est.get("work_status"),
                    "lead_source": est.get("lead_source"),
                    "company_id": est.get("company_id"),
                    "estimate_number": est.get("estimate_number"),
                }
            )
    print(f"Flattening {len(rows)} estimate-tech pairs → estimates_flat")
    sb.table("estimates_flat").upsert(rows).execute()


def flatten_jobs(items):
    sb = get_supabase()
    rows = []
    for job in items:
        job_id = job.get("id")
        assigned = job.get("assigned_employees", [])
        completed_at = job.get("work_timestamps", {}).get("completed_at")
        for tech in assigned:
            tech_id = tech.get("id")
            rows.append(
                {
                    "id": f"{job_id}__{tech_id}",
                    "job_id": job_id,
                    "technician_id": tech_id,
                    "created_at": job.get("created_at"),
                    "updated_at": job.get("updated_at"),
                    "completed_at": completed_at,
                    "work_status": job.get("work_status"),
                    "total_amount": job.get("total_amount"),
                    "outstanding_balance": job.get("outstanding_balance"),
                    "lead_source": job.get("lead_source"),
                    "company_id": job.get("company_id"),
                    "assigned_count": len(assigned),
                }
            )
    print(f"Flattening {len(rows)} job-tech pairs → jobs_flat")
    sb.table("jobs_flat").upsert(rows).execute()


def flatten_job_appointments(items):
    sb = get_supabase()
    rows = []
    for appt in items:
        appt_id = appt.get("id")
        for tech_id in appt.get("dispatched_employees_ids", []):
            rows.append(
                {
                    "id": f"{appt_id}__{tech_id}",
                    "job_appointment_id": appt_id,
                    "dispatched_employee_id": tech_id,
                    "start_time": appt.get("start_time"),
                    "end_time": appt.get("end_time"),
                    "arrival_window_minutes": appt.get("arrival_window_minutes"),
                }
            )
    print(f"Flattening {len(rows)} appt-tech pairs → job_appointments_flat")
    if rows:
        sb.table("job_appointments_flat").upsert(rows).execute()


def flatten_job_invoices(items):
    sb = get_supabase()
    rows = [
        {
            "id": inv.get("id"),
            "amount": inv.get("amount"),
            "status": inv.get("status"),
            "due_at": inv.get("due_at"),
            "paid_at": inv.get("paid_at"),
            "sent_at": inv.get("sent_at"),
            "invoice_date": inv.get("invoice_date"),
            "service_date": inv.get("service_date"),
            "due_amount": inv.get("due_amount"),
            "invoice_number": inv.get("invoice_number"),
        }
        for inv in items
    ]
    print(f"Flattening {len(rows)} invoices → job_invoices_flat")
    if rows:
        sb.table("job_invoices_flat").upsert(rows).execute()


def flatten_tsheets(items):
    sb = get_supabase()
    rows = []
    for ts in items:
        start = ts.get("start") or None
        end = ts.get("end") or None
        lm = ts.get("last_modified") or None

        # Map TSheets user to technician_id
        map_resp = (
            sb.table("tsheets_user_mapping")
            .select("technician_id")
            .eq("tsheets_user_id", ts.get("user_id"))
            .maybe_single()
            .execute()
        )
        tech_id = map_resp.data.get("technician_id") if map_resp and map_resp.data else None

        rows.append(
            {
                "id": ts.get("id"),
                "user_id": ts.get("user_id"),
                "technician_id": tech_id,
                "state": ts.get("state"),
                "start_time": start,
                "end_time": end,
                "duration_seconds": ts.get("duration"),
                "entry_date": ts.get("date"),
                "tz_offset": ts.get("tz"),
                "tz_name": ts.get("tz_str"),
                "entry_type": ts.get("type"),
                "location": ts.get("location"),
                "on_the_clock": ts.get("on_the_clock"),
                "locked_in_days": ts.get("locked"),
                "notes": ts.get("notes"),
                "customfields": ts.get("customfields"),
                "last_modified": lm,
                "distance_tracking": ts.get("distance_tracking", {}),
                "attached_files": ts.get("attached_files", []),
                "created_by_user_id": ts.get("created_by_user_id"),
            }
        )
    print(f"Flattening {len(rows)} tsheets entries → tsheets_time_entries")
    if rows:
        sb.table("tsheets_time_entries").upsert(rows).execute()


# ─── flattener registry
FLATTEN_MAP = {
    "employees": flatten_employees,
    "customers": flatten_customers,
    "estimates": flatten_estimates,
    "jobs": flatten_jobs,
    "job_appointments": flatten_job_appointments,
    "job_invoices": flatten_job_invoices,
    "tsheets": flatten_tsheets,
}


# ──────────────────────────────────────────────────────────────────────────────
# Ramp (unchanged)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_ramp_expenses() -> list[dict]:
    to_dt = datetime.utcnow()
    from_dt = to_dt - timedelta(days=90)
    try:
        return ramp_fetcher.fetch_all_transactions(
            from_dt.isoformat(timespec="seconds") + "Z",
            to_dt.isoformat(timespec="seconds") + "Z",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"❌ ERROR fetching ramp expenses: {exc}")
        return []


def flatten_ramp_expenses(items):
    sb = get_supabase()
    rows = []
    for exp in items:
        card_holder = exp.get("card_holder") or {}
        first = card_holder.get("first_name")
        last = card_holder.get("last_name")
        tech_id = None
        if first and last:
            try:
                mapping_resp = (
                    sb.table("hcp_raw_employees")
                    .select("id")
                    .eq("payload->>first_name", first)
                    .eq("payload->>last_name", last)
                    .maybe_single()
                    .execute()
                )
                tech_id = mapping_resp.data.get("id") if mapping_resp and mapping_resp.data else None
            except Exception:  # noqa: BLE001
                tech_id = None
        rows.append(
            {
                "id": exp.get("id"),
                "transaction_time": exp.get("user_transaction_time"),
                "amount": exp.get("amount"),
                "currency_code": exp.get("currency_code"),
                "merchant_name": exp.get("merchant_name"),
                "spend_category": exp.get("sk_category_name"),
                "status": exp.get("state"),
                "receipt_count": len(exp.get("receipts", [])),
                "synced_at": exp.get("synced_at"),
                "technician_id": tech_id,
            }
        )
    print(f"Flattening {len(rows)} ramp expenses → ramp_expenses_flat")
    if rows:
        sb.table("ramp_expenses_flat").upsert(rows).execute()


FLATTEN_MAP["ramp_expenses"] = flatten_ramp_expenses

# ──────────────────────────────────────────────────────────────────────────────
# Tsheets timesheets (unchanged logic – still syncs directly → flat table)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_all_tsheets_timesheets(
    start_date: str,
    end_date: str,
    *,
    limit: int = 200,
) -> List[Dict]:
    headers = {"Authorization": f"Bearer {TSHEETS_API_TOKEN}"}
    params = {"start_date": start_date, "end_date": end_date, "limit": limit, "page": 1}
    results: List[Dict] = []
    while True:
        resp = requests.get(f"{TSHEETS_BASE_URL}/timesheets", headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        timesheets_dict = resp.json().get("results", {}).get("timesheets", {})
        page_rows = list(timesheets_dict.values())
        if not page_rows:
            break
        results.extend(page_rows)
        params["page"] += 1
    return results


def fetch_tsheets_users():
    url = f"{TSHEETS_BASE_URL}/users"
    try:
        resp = requests.get(url, headers=TSHEETS_HEADERS, timeout=10)
        resp.raise_for_status()
        users = resp.json().get("results", {}).get("users", {}) or {}
        return list(users.values())
    except Exception as exc:  # noqa: BLE001
        print(f"❌ ERROR fetching {url}: {exc}")
        return []


def flatten_tsheets_users(items):
    sb = get_supabase()
    rows = [{"id": u.get("id"), "first_name": u.get("first_name", ""), "last_name": u.get("last_name", ""), "email": u.get("email")} for u in items]
    print(f"Flattening {len(rows)} TSheets users → tsheets_users_flat")
    sb.table("tsheets_users_flat").upsert(rows).execute()


FLATTEN_MAP["tsheets_users"] = flatten_tsheets_users

# ──────────────────────────────────────────────────────────────────────────────
# Entity registry
# ──────────────────────────────────────────────────────────────────────────────
ENTITY_CONFIG = {
    "employees": {"fetch": fetch_employees, "table": "hcp_raw_employees", "id": lambda i: i.get("id")},
    "customers": {"fetch": fetch_customers, "table": "hcp_raw_customers", "id": lambda i: i.get("id")},
    "estimates": {"fetch": fetch_estimates, "table": "hcp_raw_estimates", "id": lambda i: i.get("id")},
    "jobs": {"fetch": fetch_jobs, "table": "hcp_raw_jobs", "id": lambda i: i.get("id")},
    "job_appointments": {
        "fetch": fetch_job_appointments,
        "table": "hcp_raw_job_appointments",
        "id": lambda i: i.get("id"),
    },
    "job_invoices": {"fetch": fetch_job_invoices, "table": "hcp_raw_job_invoices", "id": lambda i: i.get("id")},
    "tsheets": {
        "fetch": lambda: fetch_tsheets_async("2022-06-12", date.today().isoformat()),
        "table": None,
        "id": lambda i: i.get("id"),
    },

    "tsheets_users": {"fetch": fetch_tsheets_users, "table": "tsheets_users_flat", "id": lambda u: u.get("id")},
    "ramp_expenses": {"fetch": fetch_ramp_expenses, "table": "ramp_raw_expenses", "id": lambda e: e.get("id")},
}

# ──────────────────────────────────────────────────────────────────────────────
# DB upsert helper
# ──────────────────────────────────────────────────────────────────────────────
def upsert(entity: str, items):
    cfg = ENTITY_CONFIG.get(entity)
    # nothing to do if this entity has no raw table
    if not cfg or cfg["table"] is None or not items:
        return

    sb = get_supabase()
    rows = [{"id": cfg["id"](itm), "payload": itm} for itm in items]
    print(f"↳ upserting {len(rows):>5} rows → {cfg['table']}")
    sb.table(cfg["table"]).upsert(rows).execute()



# ──────────────────────────────────────────────────────────────────────────────
# Public CLI helpers
# ──────────────────────────────────────────────────────────────────────────────
async def run_full_sync():
    print("▶️  [run_full_sync] STARTING full sync")
    entities = [
        "employees",
        "customers",
        "estimates",
        "jobs",
        "job_appointments",
        "job_invoices",
        "tsheets",
        "ramp_expenses",
    ]
    for ent in entities:
        print(f"🔄  Syncing {ent} …")
        items = ENTITY_CONFIG[ent]["fetch"]()
        # items may be coroutine (async paginated).  Await if needed
        if asyncio.iscoroutine(items):
            items = await items
        upsert(ent, items)
        if ent in FLATTEN_MAP:
            FLATTEN_MAP[ent](items)
    print("✅ Full ETL cycle completed")


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m app.etl <entity>")
        print("Entities:", ", ".join(ENTITY_CONFIG))
        return
    entity = sys.argv[1]
    if entity == "full":
        asyncio.run(run_full_sync())
        return
    cfg = ENTITY_CONFIG.get(entity)
    if not cfg:
        print("Unknown entity")
        return
    items = cfg["fetch"]()
    if asyncio.iscoroutine(items):
        items = asyncio.run(items)
    upsert(entity, items)
    if entity in FLATTEN_MAP:
        FLATTEN_MAP[entity](items)


if __name__ == "__main__":
    main()
