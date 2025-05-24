from flask import Blueprint, render_template

bp = Blueprint("core", __name__)

@bp.route("/")
def index():
    return render_template("index.html")

from flask import jsonify
from ..db import get_supabase

@bp.route("/debug/tables")
def debug_tables():
    sb = get_supabase()
    res = sb.rpc("list_public_tables").execute()
    tables = [row["tablename"] for row in res.data]
    return jsonify({"tables": tables})
