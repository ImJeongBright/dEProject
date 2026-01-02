with bike_data as (
    select * from {{ ref('stg_bike_realtime') }}
),

weather_data as (
    select * from {{ ref('stg_weather') }}
),


bike_calc as (
    select
        station_id,
        created_at as execution_date,
        bike_count as bike_cnt,
        rack_count as rack_cnt,

        lag(bike_count) over (partition by station_id order by created_at) as prev_bike_cnt
    from bike_data
),

bike_hourly as (
    select
        date_trunc('hour', execution_date) as datum_hour,


        avg(case when rack_cnt > 0 then (bike_cnt::float / rack_cnt) * 100 else 0 end) as avg_utilization_rate,

        sum(
            case
                when prev_bike_cnt > bike_cnt then (prev_bike_cnt - bike_cnt)
                else 0
            end
        ) as estimated_rentals
    from bike_calc
    group by 1
),


weather_pivoted as (
    select
        fcst_timestamp as datum_hour,
        max(case when category = 'TMP' then fcst_value::float end) as temp_c,


        max(case when category = 'TMP' then fcst_value::float end) as temp,
        max(case when category = 'PTY' then fcst_value::float end) as rain_type,
        max(case when category = 'POP' then fcst_value::float end) as rain_prob,
        max(case when category = 'REH' then fcst_value::float end) as humidity
    from weather_data
    group by 1
)


select
    b.datum_hour,
    b.avg_utilization_rate,
    b.estimated_rentals,
    w.temp,
    w.rain_type,
    w.rain_prob,
    w.humidity
from bike_hourly b
left join weather_pivoted w
    on b.datum_hour = w.datum_hour
order by b.datum_hour desc
