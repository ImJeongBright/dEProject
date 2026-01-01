{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT
        station_id,
        station_name,
        rack_cnt,
        bike_cnt,
        shared,
        latitude,
        longitude,
        execution_date::timestamp as created_at
    FROM {{ source('seoul_bike', 'bike_realtime') }}
)

SELECT
    DISTINCT
    -- Create a surrogate key for uniqueness testing across time
    station_id || '-' || CAST(created_at AS VARCHAR) as bike_status_id,
    station_id,
    station_name,
    CAST(rack_cnt AS INTEGER) as rack_count,
    CAST(bike_cnt AS INTEGER) as bike_count,
    CAST(shared AS INTEGER) as shared_count,
    CAST(latitude AS FLOAT) as latitude,
    CAST(longitude AS FLOAT) as longitude,
    created_at
FROM raw_data
