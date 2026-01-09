# supabase-de-platform
End-to-end data engineering project on Supabase: incremental ingestion, dbt dimensional modeling, data quality gates, and an API layer with observability.



NYC 311 Open Data API
        |
        v
Python Ingestion (pipelines/ingest_311.py)
  - incremental pull (watermark)
  - upsert (idempotent)
  - store raw JSON for traceability
        |
        v
Supabase Postgres (Bronze)
  - bronze_311_requests
  - ingestion_watermarks
  - pipeline_runs
        |
        v
dbt Transformations + Tests
  - staging: stg_bronze_311_requests (view)
  - marts: dim_agency, dim_complaint_type, fact_service_requests
  - tests: unique, not_null, relationships
        |
        v
Prefect Orchestration (pipelines/flow_311.py)
  - ingest -> dbt run -> dbt test
        |
        v
Analytics Tables / Views
  - fast SQL for reporting + dashboards



