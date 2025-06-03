# app/__init__.py
import os, atexit, logging
from flask import Flask, session, redirect, url_for, request
from flask_session import Session

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron       import CronTrigger

from app.etl import run_full_sync      # ← your existing full‑cycle ETL

# ─────────────────────────────────────────────────────────────────────
def create_app() -> Flask:
    """Flask application factory."""
    app = Flask(__name__, template_folder="templates")

    # ---- core config ------------------------------------------------
    app.config["SECRET_KEY"]   = os.getenv("FLASK_SECRET_KEY", "dev")
    app.config["SESSION_TYPE"] = "filesystem"
    Session(app)

    # ---- blueprint imports -----------------------------------------
    from .views import bp as core_bp
    from .views.dashboard import bp as dash_bp
    from .views.team      import bp as team_bp
    from .views.resources import bp as res_bp
    from .views.settings  import bp as set_bp
    from .auth            import bp as auth_bp

    # ---- register blueprints ---------------------------------------
    for bp in (core_bp, dash_bp, team_bp, res_bp, set_bp, auth_bp):
        app.register_blueprint(bp)

    # ---- APScheduler: run ETL every hour ---------------------------
    logging.getLogger("apscheduler").setLevel(logging.INFO)

    sched = BackgroundScheduler(timezone="UTC")
    sched.add_job(
        run_full_sync,           # the job function
        CronTrigger(minute=0),   # HH:00 UTC every hour
        id="hourly_etl",
        max_instances=1,         # never overlap
        coalesce=True,           # skip if previous still running
        misfire_grace_time=300   # 5‑min tolerance on restart
    )
    sched.start()
    atexit.register(lambda: sched.shutdown(wait=False))
    # ----------------------------------------------------------------

    # ---- global login guard ----------------------------------------
    @app.before_request
    def require_login():
        exempt = ("auth.login", "auth.callback", "auth.logout", "static")
        ep = request.endpoint or ""
        if ep.startswith(exempt):
            return
        if not session.get("user"):
            return redirect(url_for("auth.login"))

    return app
