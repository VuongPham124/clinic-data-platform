{{ config(materialized='table') }}

with f as (
  select *
  from {{ source('platinum', 'fact_operational_clinic_bookings_valid') }}
),

week_map as (
  select
    date_key,
    full_date,
    cast(format_date('%Y%m%d', date_trunc(full_date, week(monday))) as int64) as week_start_date_key
  from {{ source('platinum', 'dim_date') }}
),

name_cli as (
  select clinic_key, clinic_name
  from {{ source('platinum', 'dim_clinics') }}
)

select
  abs(farm_fingerprint(concat(
    cast(f.clinic_key as string), '|',
    cast(w.week_start_date_key as string)
  ))) as id,

  f.clinic_key,
  nc.clinic_name,
  w.week_start_date_key,

  countif(f.is_completed) as completed_booking_count,
  avg(f.confirm_duration_sec) as avg_confirm_duration_sec,
  avg(f.consult_duration_sec) as avg_consult_duration_sec,
  safe_divide(countif(f.canceled_date_key=10101), count(1)) as cancellation_rate

from f
join week_map w
  on w.date_key = f.from_date_key
join name_cli nc
  on nc.clinic_key = f.clinic_key
group by 1,2,3,4
