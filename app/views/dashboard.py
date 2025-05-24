import json
from datetime import date, datetime, timedelta

from flask import Blueprint, render_template, request, jsonify, session
from ..db import get_supabase

bp = Blueprint("dash", __name__, url_prefix="/dashboard")

# ─── Helpers ────────────────────────────────────────────────────────────────

def _get_date_range(period: str, start_str: str, end_str: str):
    today = date.today()
    if period == "custom" and start_str and end_str:
        start_date = datetime.fromisoformat(start_str).date()
        end_date = datetime.fromisoformat(end_str).date()
    elif period == "current_week":
        start_date = today - timedelta(days=today.weekday())
        end_date = today
    elif period == "last_week":
        end_date = today - timedelta(days=today.weekday() + 1)
        start_date = end_date - timedelta(days=6)
    elif period == "month_to_date":
        start_date = today.replace(day=1)
        end_date = today
    elif period == "last_month":
        first = today.replace(day=1)
        last = first - timedelta(days=1)
        start_date = last.replace(day=1)
        end_date = last
    elif period == "quarter_to_date":
        q = (today.month - 1) // 3
        start_month = q * 3 + 1
        start_date = date(today.year, start_month, 1)
        end_date = today
    elif period == "last_quarter":
        q1 = (today.month - 1) // 3
        first_q = date(today.year, q1 * 3 + 1, 1)
        last = first_q - timedelta(days=1)
        q0 = (last.month - 1) // 3
        start_date = date(last.year, q0 * 3 + 1, 1)
        end_date = last
    elif period == "year_to_date":
        start_date = today.replace(month=1, day=1)
        end_date = today
    elif period == "last_year":
        start_date = date(today.year - 1, 1, 1)
        end_date = date(today.year - 1, 12, 31)
    else:
        start_date = today
        end_date = today
    return start_date.isoformat(), end_date.isoformat()

# ─── Dashboard View ─────────────────────────────────────────────────────────

@bp.route("/")
def dashboard():
    # 1. Define KPIs and periods
    kpis = [
        ("total_opportunities", "Total Opportunities"),
        ("total_jobs", "Total Jobs"),
        ("conversion_rate", "Conversion Rate"),
        ("avg_ticket", "Average Ticket"),
        ("total_revenue", "Total Revenue"),
        ("memberships_sold", "Memberships Sold"),
        ("hours_worked", "Hours Worked"),
        ("sold_hours", "Sold Hours"),
        ("hour_efficiency", "Sold Hour Efficiency"),
    ]
    time_options = [
        ("current_week", "Current Week"),
        ("last_week", "Last Week"),
        ("month_to_date", "Month to Date"),
        ("last_month", "Last Month"),
        ("quarter_to_date", "Quarter to Date"),
        ("last_quarter", "Last Quarter"),
        ("year_to_date", "Year to Date"),
        ("last_year", "Last Year"),
        ("custom", "Custom Date Range"),
    ]

    # 2. Read selections
    selected_kpis = request.args.getlist("kpi") or [key for key, _ in kpis]
    selected_period = request.args.get("period", "current_week")
    sd_str = request.args.get("start_date", "")
    ed_str = request.args.get("end_date", "")

    # 3. Compute date range
    start_iso, end_iso = _get_date_range(selected_period, sd_str, ed_str)

    # 4. Initialize KPI values
    kpi_values = {key: 0 for key, _ in kpis}
    sb = get_supabase()

    # 5. Lookup employee ID
    user_email = session.get("user", {}).get("email")
    emp_resp = (
        sb.table("hcp_raw_employees")
          .select("id")
          .eq("payload->>email", user_email)
          .maybe_single()
          .execute()
    )
    emp_id = emp_resp.data.get("id") if emp_resp and emp_resp.data else None
    print("DEBUG: emp_id =", emp_id)

    # 6. DEBUG: inspect sample estimates
    sample_resp = sb.table("hcp_raw_estimates").select("payload").limit(3).execute()
    for idx, row in enumerate(sample_resp.data):
        payload = row.get("payload", {})
        print(f"DEBUG SAMPLE[ {idx} ] keys=", list(payload.keys()), 
              "technician_ids=", payload.get("technician_ids"),
              "start_time=", payload.get("start_time"))

            # 7. Total Opportunities
    if emp_id:
        # count estimates assigned to this technician that were created within the date range
        opp_resp = (
            sb.table("hcp_raw_estimates")
              .select("id", count="exact")
              .filter("payload->assigned_employees", "cs", f'[{{"id":"{emp_id}"}}]')
              .gte("payload->>created_at", f"{start_iso}T00:00:00Z")
              .lte("payload->>created_at", f"{end_iso}T23:59:59Z")
              .execute()
        )
        kpi_values["total_opportunities"] = opp_resp.count or 0

    # 8. Fetch saved layout Fetch saved layout Fetch saved layout
    layout_resp = (
        sb.table("user_layouts")
          .select("layout_json")
          .eq("user_id", session.get("user", {}).get("id"))
          .maybe_single()
          .execute()
    )
    saved_order = []
    if layout_resp and layout_resp.data:
        saved_order = layout_resp.data.get("layout_json") or []

    # 9. Render
    return render_template(
        "dashboard.html",
        kpis=kpis,
        time_options=time_options,
        selected_kpis=selected_kpis,
        selected_period=selected_period,
        start_date=start_iso,
        end_date=end_iso,
        kpi_values=kpi_values,
        saved_order=saved_order,
    )

# Layout save/reset routes unchanged

@bp.route("/layout", methods=["POST"])
def save_layout():
    user = session.get("user") or {}
    user_id = user.get("id")
    if not user_id:
        return jsonify({"error": "Not signed in"}), 401
    order = request.json.get("order", [])
    sb = get_supabase()
    sb.table("user_layouts").upsert({"user_id": user_id, "layout_json": json.dumps(order)}).execute()
    return jsonify({"status": "ok"})

@bp.route("/layout/reset", methods=["POST"])
def reset_layout():
    user = session.get("user") or {}
    user_id = user.get("id")
    if not user_id:
        return jsonify({"error": "Not signed in"}), 401
    sb = get_supabase()
    sb.table("user_layouts").delete().eq("user_id", user_id).execute()
    return jsonify({"status": "ok"})
