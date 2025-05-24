import os
from flask import Blueprint, redirect, url_for, session, request, render_template, jsonify
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
import google.auth.transport.requests as grequests

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

bp = Blueprint("auth", __name__, url_prefix="/auth")

GOOGLE_CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]

def _make_flow():
    return Flow.from_client_config(
        {
            "web": {
                "client_id":     GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
                "token_uri":     "https://oauth2.googleapis.com/token",
            }
        },
        scopes=[
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ],
        redirect_uri=url_for("auth.callback", _external=True, _scheme="https"),
    )

@bp.route("/login")
def login():
    # Build the Google OAuth URL but render our branded page
    flow      = _make_flow()
    auth_url, _ = flow.authorization_url(
        prompt="consent",
        include_granted_scopes="true"
    )
    # Serve login.html with the Google button URL
    return render_template("login.html", auth_url=auth_url)

@bp.route("/callback")
def callback():
    flow = _make_flow()
    flow.fetch_token(authorization_response=request.url)

    # Verify ID token & pull user info
    credentials     = flow.credentials
    request_session = grequests.Request()
    idinfo          = id_token.verify_oauth2_token(
        credentials._id_token, request_session, GOOGLE_CLIENT_ID
    )

    # Save a minimal user dict in session
    session["user"] = {
        "id":      idinfo["sub"],
        "email":   idinfo["email"],
        "name":    idinfo.get("name"),
        "picture": idinfo.get("picture"),
    }

    # After login, send them to home or dashboard
    return redirect(url_for("dash.dashboard"))

@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
