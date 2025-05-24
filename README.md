# HGFI Performance Center

A Flask-based internal dashboard for Honey Go Fix It technicians and staff.  
Pulls raw JSON payloads from HouseCall Pro, flattens them into Postgres tables, and exposes per-user KPIs (Total Opportunities, Jobs, Conversion Rate, etc.) in a drag-and-drop widget layout.

---

## 🚀 Features

- **Authentication**  
  Google OAuth sign-in, restricted to `@honeygofixit.com` accounts.  
- **Customizable Dashboard**  
  - Drag-and-drop KPI cards  
  - “Edit” / “Standard” toggle  
  - Per-user saved layouts in Supabase  
- **Time-window filtering**  
  Current week, last week, MTD, QTD, YTD, All-Time, or custom date range  
- **ETL pipeline**  
  - Fetches from HouseCall Pro REST API  
  - Stores raw JSON payloads in `hcp_raw_*` tables  
  - Flattens into `*_flat` tables for fast filtering & aggregation  
- **Accurate KPI counts**  
  - Uses scheduled-start timestamp  
  - Timezone-aware UTC windowing  
  - Filters out cancelled estimates/jobs  

---

## 📦 Tech Stack

- **Backend**: Python 3.11, Flask, Flask-Session  
- **DB**: Supabase (Postgres + PostgREST)  
- **Auth**: Google OAuth2 via `google-auth-oauthlib`  
- **Frontend**: Jinja2 templates, TailwindCSS + Flowbite, [SortableJS] for drag-drop  
- **ETL**: `requests`, `aiohttp`  
- **Deployment**: Gunicorn  

---

## 🛠️ Setup & Local Development

1. **Clone**  
   ```bash
   git clone https://github.com/your-username/HGFI-Performance-Center.git
   cd HGFI-Performance-Center

Project structure
.
├── app/
│   ├── __init__.py          # Flask app factory
│   ├── auth.py              # Google OAuth routes
│   ├── db.py                # Supabase client
│   ├── etl.py               # Pull, upsert & flatten scripts
│   └── views/
│       ├── dashboard.py     # KPI view & layout logic
│       ├── resources.py
│       ├── team.py
│       └── settings.py
├── migrations/              # Supabase SQL migrations
├── scripts/
│   └── sample_keys.py       # helper to inspect raw JSON shape
├── static/                  # CSS, JS, images (bee boy mascot, etc.)
├── templates/
│   ├── dashboard.html
│   ├── login.html
│   └── …  
├── README.md
├── pyproject.toml
└── main.py                  # entry-point
