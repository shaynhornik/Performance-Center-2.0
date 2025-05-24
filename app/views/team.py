from flask import Blueprint, render_template
bp = Blueprint("team", __name__, url_prefix="/team")

@bp.route("/performance")
def performance():
    return render_template("team_performance.html")
