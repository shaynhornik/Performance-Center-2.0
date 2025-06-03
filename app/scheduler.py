"""
Lightweight ETL orchestrator for Honey Go Fix It.
Runs inside Replit; uses APScheduler + Supabase pg_cron checkpoints.
"""

import asyncio, os
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.db import get_supabase
from app import etl                       # your existing ETL module

TZ = timezone.utc        # keep UTC in DB; convert only for display

SB = get_supabase()
GRACE = timedelta(minutes=5)              # overlap window


def _get_checkpoint(entity: str) -> datetime:
    res = SB.table("etl_checkpoints").select("last_synced_at")\
              .eq("entity", entity).maybe_single().execute()
    return res.data["last_synced_at"] if res and res.data else datetime(1970,1,1,tzinfo=TZ)


def _set_checkpoint(entity: str, ts: datetime):
    SB.table("etl_checkpoints").upsert({"entity": entity,
                                        "last_synced_at": ts}).execute()


async def sync(entity: str):
    start = _get_checkpoint(entity) - GRACE
    end   = datetime.now(tz=TZ)
    print(f"🔄  Syncing {entity} from {start} to {end}")

    # call the right ETL fetcher
    if entity == "ramp_expenses":
        items = etl.ramp_fetcher.fetch_all_transactions(
                   start.isoformat(), end.isoformat())
    elif entity == "tsheets":
        items = etl.fetch_all_tsheets_timesheets(
                   start.date().isoformat(), end.date().isoformat())
    elif entity == "hcp":
        items = etl.fetch_jobs()          # temporary – no delta API yet
    else:
        raise ValueError(f"Unknown entity {entity}")

    # raw‑upsert + flatten via existing helpers
    etl.upsert(entity, items)
    etl.FLATTEN_MAP[entity](items)

    _set_checkpoint(entity, end)
    print(f"✅  {entity} sync complete ({len(items)} records)")


async def run_hourly():
    await sync("hcp")
    await sync("tsheets")
    await sync("ramp_expenses")


def start_scheduler():
    sched = AsyncIOScheduler(timezone="America/New_York")
    # Hourly 06:00‑22:00 ET
    sched.add_job(run_hourly, "cron", hour="6-22", minute=0)
    # Deep sweep once a night
    sched.add_job(run_hourly, "cron", hour=3, minute=0)
    sched.start()
    print("🕰️  Scheduler started…")


if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI

    start_scheduler()                     # keep APScheduler alive

    app = FastAPI()

    @app.get("/health")
    def health(): return {"status": "ok", "time": datetime.now()}

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
