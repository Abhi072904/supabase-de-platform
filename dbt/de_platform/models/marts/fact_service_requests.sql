select
  r.unique_key,
  r.created_at,
  r.updated_at,
  md5(coalesce(r.agency, '')) as agency_key,
  md5(coalesce(r.complaint_type, '')) as complaint_type_key,
  r.descriptor,
  r.status,
  r.borough,
  r.incident_zip,
  r.latitude,
  r.longitude
from {{ ref('stg_bronze_311_requests') }} r
