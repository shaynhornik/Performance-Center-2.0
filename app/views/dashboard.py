import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Blueprint, render_template, request, jsonify, session
from ..db import get_supabase

bp = Blueprint("dash", __name__, url_prefix="/dashboard")

def _get_date_range(period: str, start_str: str, end_str: str):
    today = date.today()
    if period == "custom" and start_str and end_str:
        start_date = datetime.fromisoformat(start_str).date()
        end_date   = datetime.fromisoformat(end_str).date()
    elif period == "current_week":
        start_date = today - timedelta(days=today.weekday())
        end_date   = today
    elif period == "last_week":
        end_date   = today - timedelta(days=today.weekday() + 1)
        start_date = end_date - timedelta(days=6)
    elif period == "month_to_date":
        start_date = today.replace(day=1)
        end_date   = today
    elif period == "last_month":
        first      = today.replace(day=1)
        last       = first - timedelta(days=1)
        start_date = last.replace(day=1)
        end_date   = last
    elif period == "quarter_to_date":
        q           = (today.month - 1) // 3
        start_month = q * 3 + 1
        start_date  = date(today.year, start_month, 1)
        end_date    = today
    elif period == "last_quarter":
        q1          = (today.month - 1) // 3
        first_q     = date(today.year, q1 * 3 + 1, 1)
        last        = first_q - timedelta(days=1)
        q0          = (last.month - 1) // 3
        start_date  = date(last.year, q0 * 3 + 1, 1)
        end_date    = last
    elif period == "year_to_date":
        start_date = today.replace(month=1, day=1)
        end_date   = today
    elif period == "last_year":
        start_date = date(today.year - 1, 1, 1)
        end_date   = date(today.year - 1, 12, 31)
    else:
        start_date = today
        end_date   = today

    return start_date.isoformat(), end_date.isoformat()


@bp.route("/")
def dashboard():
    # 1) KPIs & periods
    kpis = [
        ("total_opportunities", "Total Opportunities"),
        ("total_jobs",           "Completed Jobs"),
        ("conversion_rate",      "Conversion Rate"),
        ("avg_ticket",           "Average Ticket"),
        ("total_revenue",        "Total Revenue"),
        ("attributed_revenue",   "Attributed Revenue"),
        ("memberships_sold",     "Memberships Sold"),
        ("hours_worked",         "Hours Worked"),
        ("sold_hours",           "Sold Hours"),
        ("hour_efficiency",      "Sold Hour Efficiency"),
    ]
    time_options = [
        ("current_week",   "Current Week"),
        ("last_week",      "Last Week"),
        ("month_to_date",  "Month to Date"),
        ("last_month",     "Last Month"),
        ("quarter_to_date","Quarter to Date"),
        ("last_quarter",   "Last Quarter"),
        ("year_to_date",   "Year to Date"),
        ("last_year",      "Last Year"),
        ("all_time",       "All Time"),
        ("custom",         "Custom Date Range"),
    ]

    # 2) Read selections
    selected_kpis   = request.args.getlist("kpi") or [key for key, _ in kpis]
    selected_period = request.args.get("period", "current_week")
    sd_str          = request.args.get("start_date", "")
    ed_str          = request.args.get("end_date", "")

    # 3) Build UTC date‐window
    start_iso, end_iso = _get_date_range(selected_period, sd_str, ed_str)
    local_tz           = ZoneInfo("America/New_York")
    start_local        = datetime.fromisoformat(start_iso).replace(tzinfo=local_tz)
    end_local          = datetime.fromisoformat(end_iso).replace(tzinfo=local_tz)
    start_utc          = start_local.astimezone(ZoneInfo("UTC"))
    end_utc_incl       = end_local.astimezone(ZoneInfo("UTC")) + timedelta(days=1)
    start_iso_utc      = start_utc.isoformat().replace("+00:00", "Z")
    end_iso_next_utc   = end_utc_incl.isoformat().replace("+00:00", "Z")

    # 4) Init
    kpi_values = { key: 0 for key, _ in kpis }
    sb = get_supabase()

    # 5) Who’s logged in?
    user_email = (session.get("user") or {}).get("email", "").lower()
    emp_resp   = (
        sb.table("hcp_raw_employees")
          .select("id")
          .filter("payload->>email", "ilike", user_email)
          .maybe_single()
          .execute()
    )
    emp_id = emp_resp.data.get("id") if emp_resp and emp_resp.data else None

    if emp_id:
        # ─── Total Opportunities ───────────────────────────────────────
        # fetch assignments in window (or since launch for all_time)
        q = (
            sb.table("estimate_assigned_employees_flat")
              .select("estimate_id", count="exact")
              .eq("technician_id", emp_id)
        )
        if selected_period == "all_time":
            q = q.gte("scheduled_start", "2022-06-12T00:00:00Z")
        else:
            q = q.gte("scheduled_start", start_iso_utc).lt("scheduled_start", end_iso_next_utc)
        resp = q.execute()
        estimate_ids = { r["estimate_id"] for r in (resp.data or []) }

        # filter out fully‐canceled, re‐include any with a live option…
        # (your existing logic here)
        # …
        # finally:
        kpi_values["total_opportunities"] = len(estimate_ids)

        # ─── Completed Jobs ────────────────────────────────────────────
        jobs_q = (
            sb.table("jobs_flat")
              .select("job_id", count="exact")
              .eq("technician_id", emp_id)
        )
        if selected_period == "all_time":
            jobs_q = jobs_q.gte("completed_at", "2022-06-12T00:00:00Z")
        else:
            jobs_q = jobs_q.gte("completed_at", f"{start_iso_utc}T00:00:00Z") \
                          .lt("completed_at", f"{end_iso_next_utc}T00:00:00Z")
        jobs_resp = jobs_q.execute()
        kpi_values["total_jobs"] = jobs_resp.count or 0

        # ─── Completed Revenue ────────────────────────────────────────
        rev_q = (
            sb.table("jobs_flat")
              .select("total_amount, completed_at, work_status")
              .eq("technician_id", emp_id)
        )
        if selected_period != "all_time":
            rev_q = rev_q.gte("completed_at", f"{start_iso_utc}T00:00:00Z") \
                         .lt("completed_at", f"{end_iso_next_utc}T00:00:00Z")
        rev_rows = rev_q.execute().data or []
        # drop canceled
        rev_rows = [
            r for r in rev_rows
            if r["work_status"] not in ("pro canceled", "user canceled")
        ]
        total_rev_cents = sum((r["total_amount"] or 0) for r in rev_rows)
        kpi_values["total_revenue"] = total_rev_cents / 100.0

        # ─── Attributed Revenue ───────────────────────────────────────
        # now split each job’s revenue evenly
        rev_q2 = (
            sb.table("jobs_flat")
              .select("total_amount, assigned_count, completed_at, work_status")
              .eq("technician_id", emp_id)
        )
        if selected_period != "all_time":
            rev_q2 = rev_q2.gte("completed_at", f"{start_iso_utc}T00:00:00Z") \
                           .lt("completed_at", f"{end_iso_next_utc}T00:00:00Z")
        rev2_rows = rev_q2.execute().data or []
        # drop canceled
        rev2_rows = [
            r for r in rev2_rows
            if r["work_status"] not in ("pro canceled", "user canceled")
        ]
        total_attr_cents = sum(
            (r["total_amount"] or 0) / (r["assigned_count"] or 1)
            for r in rev2_rows
        )
        kpi_values["attributed_revenue"] = total_attr_cents / 100.0

        # ─── Conversion Rate & Avg Ticket ────────────────────────────
        opp = kpi_values["total_opportunities"]
        jobs = kpi_values["total_jobs"]
        kpi_values["conversion_rate"] = round((jobs / opp * 100) if opp else 0.0, 2)
        kpi_values["avg_ticket"] = (
            (kpi_values["total_revenue"] / jobs) if jobs else 0.0
        )

        # ─── 6.5) Hours Worked ────────────────────────────────
    hrs_q = (
        sb
        .table("tsheets_time_entries")
        .select("duration_seconds", count="exact")
        .eq("technician_id", emp_id)
    )
    if selected_period == "all_time":
        # same launch‐date cutoff
        hrs_q = hrs_q.gte("entry_date", "2022-06-12")
    else:
        hrs_q = hrs_q.gte("entry_date", start_iso).lte("entry_date", end_iso)
    hrs_resp = hrs_q.execute()
    # sum up all the duration_seconds
    total_seconds = sum(r["duration_seconds"] or 0 for r in (hrs_resp.data or []))
    # convert to hours
    kpi_values["hours_worked"] = round(total_seconds / 3600, 2)

    
    # ─── Load saved layout & render ─────────────────────────────────
    layout_resp = (
        sb.table("user_layouts")
          .select("layout_json")
          .eq("user_id", (session.get("user") or {}).get("id"))
          .maybe_single()
          .execute()
    )
    saved_order = layout_resp.data.get("layout_json") or [] \
        if layout_resp and layout_resp.data else []

    return render_template(
        "dashboard.html",
        kpis=kpis,
        time_options=time_options,
        selected_kpis=selected_kpis,
        selected_period=selected_period,
        start_date=start_iso_utc,
        end_date=end_iso_next_utc,
        kpi_values=kpi_values,
        saved_order=saved_order,
    )

@bp.route("/layout", methods=["POST"])
def save_layout():
    user_id = (session.get("user") or {}).get("id")
    if not user_id:
        return jsonify({"error":"Not signed in"}), 401
    order = request.json.get("order", [])
    get_supabase().table("user_layouts") \
        .upsert({"user_id":user_id,"layout_json":json.dumps(order)}) \
        .execute()
    return jsonify({"status":"ok"})

@bp.route("/layout/reset", methods=["POST"])
def reset_layout():
    user_id = (session.get("user") or {}).get("id")
    if not user_id:
        return jsonify({"error":"Not signed in"}), 401
    get_supabase().table("user_layouts") \
        .delete().eq("user_id",user_id) \
        .execute()
    return jsonify({"status":"ok"})
