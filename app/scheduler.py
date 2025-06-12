# app/scheduler.py
"""
Scheduler for Honey Go Fix It ETL.

• Runs the *full* ETL every hour on the hour (UTC).
• Fires one “bootstrap” run ~10 s after each cold start so the first
  sync happens immediately.
• Shares the asyncio event-loop Gunicorn/Uvicorn already uses.
• Guarantees at most one ETL at a time (max_instances=1, coalesce=True).
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
import uvicorn

from app.etl import run_full_sync      # ← your coroutine


# ─────────────────────────────────────────────────────────────────────────────
# Helper that APScheduler calls
# ─────────────────────────────────────────────────────────────────────────────
def _run_etl(tag: str) -> None:
    """
    Schedule the ETL coroutine on the current event-loop
    and log the reason it was triggered.
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"🚀  Triggering {tag} sync @ {now}")
    asyncio.create_task(run_full_sync())


# ─────────────────────────────────────────────────────────────────────────────
# APScheduler configuration
# ─────────────────────────────────────────────────────────────────────────────
sched = AsyncIOScheduler(timezone="UTC")

# 1️⃣  Hourly job – fires every hour on the hour
sched.add_job(
    _run_etl,
    trigger="cron",
    minute=0,
    id="hourly_full_sync",
    args=["hourly"],
    max_instances=1,          # never overlap runs
    coalesce=True,            # collapse missed runs into one
    misfire_grace_time=90,
)

# 2️⃣  Bootstrap job – runs once ~10 s after every cold start / deploy
if not os.getenv("SKIP_BOOTSTRAP_SYNC"):
    sched.add_job(
        _run_etl,
        trigger="date",
        run_date=datetime.now(timezone.utc) + timedelta(seconds=10),
        id="bootstrap_full_sync",
        args=["bootstrap"],
    )

sched.start()
print("🕰️  Scheduler started – hourly full-sync + bootstrap queued")


# ─────────────────────────────────────────────────────────────────────────────
# Minimal FastAPI app so the process exposes something on port $PORT
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI()

@app.get("/health")
def health():
    """Simple liveness probe."""
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
