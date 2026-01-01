{{ config(materialized='table') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_bike_realtime') }}
),

ranked_stations AS (
    SELECT
        station_id,
        station_name,
        latitude,
        longitude,
        ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY created_at DESC) as rn
    FROM staging
)

SELECT
    station_id,
    station_name,
    latitude,
    longitude
FROM ranked_stations
WHERE rn = 1
