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


@bp.route("/")
def dashboard():
    # 1) KPIs & periods
    kpis = [
        ("total_opportunities", "Total Opportunities"),
        ("total_jobs", "Completed Jobs"),
        ("conversion_rate", "Conversion Rate"),
        ("avg_ticket", "Average Ticket"),
        ("total_revenue", "Total Revenue"),
        ("attributed_revenue", "Attributed Revenue"),
        ("memberships_sold", "Memberships Sold"),
        ("hours_worked", "Hours Worked"),
        ("avg_hours_week", "Avg Hrs/Week"),
        ("avg_options_per_opportunity", "Avg Options/Opportunity"),
        ("sold_hours", "Sold Hours"),
        ("hour_efficiency", "Sold Hour Efficiency"),
        # ────────── NEW: Total Spend ───────────────────
        ("total_spend", "Total Spend"),
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
        ("all_time", "All Time"),
        ("custom", "Custom Date Range"),
    ]

    # 2) Read selections
    selected_kpis = request.args.getlist("kpi") or [key for key, _ in kpis]
    selected_period = request.args.get("period", "current_week")
    sd_str = request.args.get("start_date", "")
    ed_str = request.args.get("end_date", "")

    # 3) Build UTC date-window
    start_iso, end_iso = _get_date_range(selected_period, sd_str, ed_str)
    local_tz = ZoneInfo("America/New_York")
    start_local = datetime.fromisoformat(start_iso).replace(tzinfo=local_tz)
    end_local = datetime.fromisoformat(end_iso).replace(tzinfo=local_tz)
    start_utc = start_local.astimezone(ZoneInfo("UTC"))
    end_utc_incl = end_local.astimezone(ZoneInfo("UTC")) + timedelta(days=1)
    start_iso_utc = start_utc.isoformat().replace("+00:00", "Z")
    end_iso_next_utc = end_utc_incl.isoformat().replace("+00:00", "Z")

    # 4) Init
    kpi_values = {key: 0 for key, _ in kpis}
    sb = get_supabase()

    # 5) Who’s logged in?
    user_email = (session.get("user") or {}).get("email", "").lower()
    emp_resp = (
        sb.table("hcp_raw_employees")
        .select("id")
        .filter("payload->>email", "ilike", user_email)
        .maybe_single()
        .execute()
    )
    emp_id = emp_resp.data.get("id") if emp_resp and emp_resp.data else None

    if emp_id:
        # ─── Total Opportunities ─────────────────────────────────
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
        estimate_ids = {r["estimate_id"] for r in (resp.data or [])}
        kpi_values["total_opportunities"] = len(estimate_ids)

        # ─── Completed Jobs ───────────────────────────────────────
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

        # ─── Completed Revenue ─────────────────────────────────────
        rev_q = (
            sb.table("jobs_flat")
            .select("total_amount, completed_at, work_status")
            .eq("technician_id", emp_id)
        )
        if selected_period != "all_time":
            rev_q = rev_q.gte("completed_at", f"{start_iso_utc}T00:00:00Z") \
                         .lt("completed_at", f"{end_iso_next_utc}T00:00:00Z")
        rev_rows = rev_q.execute().data or []
        rev_rows = [
            r for r in rev_rows
            if r["work_status"] not in ("pro canceled", "user canceled")
        ]
        total_rev_cents = sum((r["total_amount"] or 0) for r in rev_rows)
        kpi_values["total_revenue"] = total_rev_cents / 100.0

        # ─── Attributed Revenue ────────────────────────────────────
        rev_q2 = (
            sb.table("jobs_flat")
            .select("total_amount, assigned_count, completed_at, work_status")
            .eq("technician_id", emp_id)
        )
        if selected_period != "all_time":
            rev_q2 = rev_q2.gte("completed_at", f"{start_iso_utc}T00:00:00Z") \
                           .lt("completed_at", f"{end_iso_next_utc}T00:00:00Z")
        rev2_rows = rev_q2.execute().data or []
        rev2_rows = [
            r for r in rev2_rows
            if r["work_status"] not in ("pro canceled", "user canceled")
        ]
        total_attr_cents = sum(
            (r["total_amount"] or 0) / (r["assigned_count"] or 1)
            for r in rev2_rows
        )
        kpi_values["attributed_revenue"] = total_attr_cents / 100.0

        # ─── Conversion Rate & Avg Ticket ─────────────────────────
        opp = kpi_values["total_opportunities"]
        jobs = kpi_values["total_jobs"]
        kpi_values["conversion_rate"] = round((jobs / opp * 100) if opp else 0.0, 2)
        kpi_values["avg_ticket"] = (
            (kpi_values["total_revenue"] / jobs) if jobs else 0.0
        )

        # ─── Total Spend (new) ────────────────────────────────────
        # We want all statuses (pending, cleared, etc.)
        spend_q = (
            sb
            .table("ramp_expenses_flat")
            .select("amount, transaction_time", count="exact")
            .eq("technician_id", emp_id)
        )
        if selected_period != "all_time":
            spend_q = (
                spend_q
                .gte("transaction_time", start_iso_utc)
                .lt("transaction_time", end_iso_next_utc)
            )

        spend_resp = spend_q.execute()
        spend_rows = spend_resp.data or []

        # DEBUG: show how many rows and a small sample
        print(f"DEBUG → fetched {len(spend_rows)} ramp expense rows for tech {emp_id}")
        if spend_rows:
            print("DEBUG → sample spend rows:", spend_rows[:3])

        # Sum up the “amount” field (in dollars, e.g. 27.38)
        total_amount = sum((row.get("amount") or 0) for row in spend_rows)
        # Store as a raw float; HTML will format it
        kpi_values["total_spend"] = total_amount

    # ─── 6.5 Hours Worked ────────────────────────────────────
    hrs_q = (
        sb
        .table("tsheets_time_entries")
        .select("duration_seconds", count="exact")
        .eq("technician_id", emp_id)
    )
    if selected_period == "all_time":
        hrs_q = hrs_q.gte("entry_date", "2022-06-12")
    else:
        hrs_q = hrs_q.gte("entry_date", start_iso).lte("entry_date", end_iso)
    hrs_resp = hrs_q.execute()
    total_seconds = sum(r["duration_seconds"] or 0 for r in (hrs_resp.data or []))
    kpi_values["hours_worked"] = round(total_seconds / 3600, 2)

    # ─── 6.6 Compute Avg Hrs/Week ───────────────────────────
    start_date = datetime.fromisoformat(start_iso).date()
    end_date = datetime.fromisoformat(end_iso).date()
    days_count = (end_date - start_date).days + 1
    weeks = days_count / 7
    avg_per_wk = round(kpi_values["hours_worked"] / weeks, 2) if weeks else 0
    kpi_values["avg_hours_week"] = avg_per_wk

    # ─── 6.7 Avg Options per Opportunity ─────────────────────
    est_resp = (
        sb
        .table("estimates_flat")
        .select("id")
        .gte("scheduled_start", start_iso)
        .lte("scheduled_start", end_iso)
        .execute()
    )
    estimate_ids = [row["id"] for row in (est_resp.data or [])]
    num_opps = len(estimate_ids)

    total_opts = 0
    chunk_size = 500
    for i in range(0, num_opps, chunk_size):
        chunk_ids = estimate_ids[i : i + chunk_size]
        opt_resp = (
            sb
            .table("estimate_options_flat")
            .select("option_id", count="exact")
            .in_("estimate_id", chunk_ids)
            .execute()
        )
        total_opts += opt_resp.count or len(opt_resp.data or [])
    avg_opts = (total_opts / num_opps) if num_opps else 0
    kpi_values["avg_options_per_opportunity"] = f"{avg_opts:.2f}"

    # ─── Load saved layout & render ──────────────────────────
    layout_resp = (
        sb.table("user_layouts")
        .select("layout_json")
        .eq("user_id", (session.get("user") or {}).get("id"))
        .maybe_single()
        .execute()
    )
    saved_order = (
        layout_resp.data.get("layout_json") or []
        if layout_resp and layout_resp.data
        else []
    )

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
        return jsonify({"error": "Not signed in"}), 401
    order = request.json.get("order", [])
    get_supabase().table("user_layouts") \
        .upsert({"user_id": user_id, "layout_json": json.dumps(order)}) \
        .execute()
    return jsonify({"status": "ok"})


@bp.route("/layout/reset", methods=["POST"])
def reset_layout():
    user_id = (session.get("user") or {}).get("id")
    if not user_id:
        return jsonify({"error": "Not signed in"}), 401
    get_supabase().table("user_layouts") \
        .delete().eq("user_id", user_id) \
        .execute()
    return jsonify({"status": "ok"})
