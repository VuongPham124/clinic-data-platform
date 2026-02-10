{{ config(materialized='table') }}

-- Generate date dimension based on booking date range
with bounds as (
  select
    min(date(from_ts)) as min_d,
    max(date(from_ts)) as max_d
  from (
    select safe_cast(from_time as timestamp) as from_ts
    from {{ source('silver', 'clinic_bookings') }}
  )
  where from_ts is not null
),
calendar as (
  select d as full_date
  from bounds,
  unnest(generate_date_array(min_d, max_d)) as d
)

select
  cast(format_date('%Y%m%d', full_date) as int64) as date_key,
  full_date,
  extract(year from full_date) as year,
  extract(month from full_date) as month,
  extract(week from full_date) as week_of_year,
  cast(format_date('%Y%m%d', date_trunc(full_date, month)) as int64) as month_start_date_key
from calendar
