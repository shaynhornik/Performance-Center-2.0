from flask import Blueprint, render_template
bp = Blueprint("resources", __name__, url_prefix="/resources")

@bp.route("/")
def index():
    return render_template("resources.html")
