select *
from {{ ref('stg_bike_realtime') }}
where created_at > (current_timestamp + interval '10 minutes')
