select
  unique_key,
  created_date::timestamptz as created_at,
  updated_date::timestamptz as updated_at,
  agency,
  complaint_type,
  descriptor,
  status,
  borough,
  incident_zip,
  latitude,
  longitude
from public.bronze_311_requests
