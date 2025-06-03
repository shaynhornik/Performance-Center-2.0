# app/__init__.py
import os, atexit, logging, importlib
from flask import Flask, session, redirect, url_for, request
from flask_session import Session

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ─── helper: load ETL only when first needed ───────────────────────
def _lazy_run_full_sync():
    global _run_full_sync
    if "_run_full_sync" not in globals():
        _run_full_sync = importlib.import_module("app.etl").run_full_sync
    return _run_full_sync()

# ─── Flask factory ─────────────────────────────────────────────────
def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates")

    # Core config
    app.config["SECRET_KEY"]   = os.getenv("FLASK_SECRET_KEY", "dev")
    app.config["SESSION_TYPE"] = "filesystem"
    Session(app)

    # Blueprints
    from .views import bp as core_bp
    from .views.dashboard import bp as dash_bp
    from .views.team      import bp as team_bp
    from .views.resources import bp as res_bp
    from .views.settings  import bp as set_bp
    from .auth            import bp as auth_bp
    for bp in (core_bp, dash_bp, team_bp, res_bp, set_bp, auth_bp):
        app.register_blueprint(bp)

    # ── Health endpoint for Replit LB ───────────────────────────────
    @app.get("/healthz")
    def healthz():
        return "ok", 200
    # ----------------------------------------------------------------

    # ── APScheduler: hourly ETL at HH:00 UTC ─────────────────────────
    logging.getLogger("apscheduler").setLevel(logging.INFO)
    sched = BackgroundScheduler(timezone="UTC")
    sched.add_job(
        _lazy_run_full_sync,             # wrapper that imports on‑demand
        CronTrigger(minute=0),           # top of every hour
        id="hourly_etl",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300
    )
    sched.start()
    atexit.register(lambda: sched.shutdown(wait=False))
    # ----------------------------------------------------------------

    # Login guard
    @app.before_request
    def require_login():
        exempt = (
            "auth.login", "auth.callback", "auth.logout",
            "static", "healthz"           # allow LB pings straight through
        )
        if (request.endpoint or "").startswith(exempt):
            return
        if not session.get("user"):
            return redirect(url_for("auth.login"))

    return app
