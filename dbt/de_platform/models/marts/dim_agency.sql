select
  md5(coalesce(agency, '')) as agency_key,
  agency as agency_name
from {{ ref('stg_bronze_311_requests') }}
where agency is not null
group by 1, 2
