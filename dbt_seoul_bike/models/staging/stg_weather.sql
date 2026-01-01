with source as (
    select * from {{ source('seoul_bike', 'weather_realtime') }}
),

ranked as (
    select
        *,
        -- Create a timestamp for forecast target time for easier joining
        to_timestamp(fcst_date || fcst_time, 'YYYYMMDDHH24MI') as fcst_timestamp,
        -- Rank to find the latest prediction for a given target time
        row_number() over (
            partition by fcst_date, fcst_time, category 
            order by base_date desc, base_time desc
        ) as rn
    from source
),

final as (
    select
        base_date,
        base_time,
        fcst_date,
        fcst_time,
        fcst_timestamp,
        category,
        fcst_value
    from ranked
    where rn = 1
)

select * from final
