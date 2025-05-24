import os
import sys
# make sure the project root is on the import path
sys.path.append(os.getcwd())

from app.db import get_supabase

def sample_payload_keys(table_name, limit=5):
    sb = get_supabase()
    resp = sb.table(table_name).select("payload").limit(limit).execute()
    return [list(row["payload"].keys()) for row in (resp.data or [])]

if __name__ == "__main__":
    tables = [
        "hcp_raw_employees",
        "hcp_raw_customers",
        "hcp_raw_estimates",
        "hcp_raw_jobs",
        "hcp_raw_job_appointments",
        "hcp_raw_job_invoices",
    ]
    for tbl in tables:
        print(f"\nTop‐level keys in {tbl}:")
        samples = sample_payload_keys(tbl)
        for idx, keys in enumerate(samples, start=1):
            print(f"  Sample {idx}: {keys}")