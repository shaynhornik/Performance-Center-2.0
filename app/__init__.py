import os
from flask import Flask, session, redirect, url_for, request
from flask_session import Session

def create_app() -> Flask:
    """Application factory: builds and returns a configured Flask app."""
    app = Flask(__name__, template_folder="templates")

    # ---- core config ---------------------------------------------
    app.config["SECRET_KEY"]   = os.environ.get("FLASK_SECRET_KEY", "dev")
    app.config["SESSION_TYPE"] = "filesystem"
    Session(app)
    
    # ---- blueprint imports *inside* the factory ------------------
    from .views import bp as core_bp                 # "/"
    from .views.dashboard import bp as dash_bp       # "/dashboard"
    from .views.team import bp as team_bp            # "/team/performance"
    from .views.resources import bp as res_bp        # "/resources"
    from .views.settings import bp as set_bp         # "/settings"
    from .auth import bp as auth_bp                  # "/auth/*"

    # ---- register blueprints -------------------------------------
    app.register_blueprint(core_bp)
    app.register_blueprint(dash_bp)
    app.register_blueprint(team_bp)
    app.register_blueprint(res_bp)
    app.register_blueprint(set_bp)
    app.register_blueprint(auth_bp)

    # Global login guard
    @app.before_request
    def require_login():
        # Allow the auth routes and static files
        exempt_endpoints = ("auth.login", "auth.callback", "auth.logout", "static")
        ep = request.endpoint or ""
        if ep.startswith(exempt_endpoints):
            return  # let them through

        # Everything else requires a logged-in user
        if not session.get("user"):
            return redirect(url_for("auth.login"))
    
    return app
