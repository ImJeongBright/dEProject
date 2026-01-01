{{ config(materialized='table') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_bike_realtime') }}
),

dim_station AS (
    SELECT station_id FROM {{ ref('dim_station') }}
)

SELECT
    s.created_at,
    s.station_id,
    s.rack_count,
    s.bike_count,
    -- Calculate usage rate (avoid division by zero)
    CASE 
        WHEN s.rack_count = 0 THEN 0
        ELSE (s.bike_count::float / s.rack_count::float) * 100
    END as load_rate
FROM staging s
JOIN dim_station d ON s.station_id = d.station_id
