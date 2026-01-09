select
  md5(coalesce(complaint_type, '')) as complaint_type_key,
  complaint_type
from {{ ref('stg_bronze_311_requests') }}
where complaint_type is not null
group by 1, 2
