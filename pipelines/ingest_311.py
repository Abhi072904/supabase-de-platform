import os
import json
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from pathlib import Path

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"  # repo root
load_dotenv(dotenv_path=ENV_PATH)

from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def utcnow():
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def soql_ts(dt: datetime) -> str:
    # Socrata SoQL commonly expects timestamps without timezone offsets
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S")


def get_watermark(sb):
    row = (
        sb.table("ingestion_watermarks")
        .select("last_created_date")
        .eq("source_name", "nyc_311")
        .single()
        .execute()
    )
    last = row.data["last_created_date"]
    # Supabase returns ISO string; parse safely
    return datetime.fromisoformat(last.replace("Z", "+00:00"))

def set_watermark(sb, dt: datetime):
    sb.table("ingestion_watermarks").upsert(
        {"source_name": "nyc_311", "last_created_date": iso(dt)}
    ).execute()

def create_run(sb, flow_name: str):
    run = sb.table("pipeline_runs").insert({"flow_name": flow_name}).execute()
    return run.data[0]["run_id"]

def finish_run(sb, run_id, status, rows_loaded=0, max_watermark=None, error=None):
    payload = {
        "status": status,
        "finished_at": iso(utcnow()),
        "rows_loaded": rows_loaded,
        "max_watermark": iso(max_watermark) if max_watermark else None,
        "error": error,
    }
    sb.table("pipeline_runs").update(payload).eq("run_id", run_id).execute()

def fetch_311_since(last_created_date: datetime, limit: int = 2000):
    # Incremental ingestion by created_date
    params = {
        "$limit": limit,
        "$order": "created_date ASC",
        "$where": f"created_date > '{soql_ts(last_created_date)}'",

    }
    resp = requests.get(DATASET_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def normalize_record(r: dict) -> dict:
    # unique_key is the natural key
    def get_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def get_dt(v):
        if not v:
            return None
        # Socrata returns ISO; ensure tz-aware
        return datetime.fromisoformat(v.replace("Z", "+00:00"))

    unique_key = int(r["unique_key"])
    created_date = get_dt(r.get("created_date"))
    updated_date = get_dt(r.get("updated_date"))

    return {
        "unique_key": unique_key,
        "created_date": iso(created_date) if created_date else None,
        "updated_date": iso(updated_date) if updated_date else None,
        "agency": r.get("agency"),
        "complaint_type": r.get("complaint_type"),
        "descriptor": r.get("descriptor"),
        "status": r.get("status"),
        "borough": r.get("borough"),
        "incident_zip": r.get("incident_zip"),
        "latitude": get_float(r.get("latitude")),
        "longitude": get_float(r.get("longitude")),
        "raw": r,  # jsonb
    }

def upsert_bronze(sb, records: list[dict]) -> int:
    if not records:
        return 0
    # Upsert by primary key unique_key
    sb.table("bronze_311_requests").upsert(records).execute()
    return len(records)

def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_id = create_run(sb, "ingest_nyc_311_bronze")

    try:
        last = get_watermark(sb)
        data = fetch_311_since(last, limit=5000)

        records = [normalize_record(r) for r in data if "unique_key" in r]
        rows = upsert_bronze(sb, records)

        max_created = last
        for rec in records:
            if rec["created_date"]:
                dt = datetime.fromisoformat(rec["created_date"])
                if dt > max_created:
                    max_created = dt

        if rows > 0 and max_created > last:
            set_watermark(sb, max_created)

        finish_run(sb, run_id, "success", rows_loaded=rows, max_watermark=max_created)
        print(f"Loaded {rows} rows. Watermark now {max_created.isoformat()}")

    except Exception as e:
        finish_run(sb, run_id, "failed", error=str(e))
        raise

if __name__ == "__main__":
    main()
