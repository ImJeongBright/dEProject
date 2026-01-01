-- Test that created_at is not in the future (allowing for small clock skew, e.g., 1 day ahead is definitely wrong)
-- We check if created_at > current_timestamp
select *
from {{ ref('stg_bike_realtime') }}
where created_at > (current_timestamp + interval '10 minutes')
-- Allowing 10 mins buffer for potential server time differences, but generally it shouldn't be future.
-- If checking exact fetch time, strictly > current_timestamp is fine.
