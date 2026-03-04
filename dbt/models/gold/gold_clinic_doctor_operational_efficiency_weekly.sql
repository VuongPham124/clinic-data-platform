{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    partition_by={"field": "week_start_date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key']
) }}

with f as (
  select
    clinic_key,
    from_date_key,
    is_completed,
    confirm_duration_sec,
    consult_duration_sec,
    canceled_date_key
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
),

base as (
  select
    f.clinic_key,
    w.week_start_date_key,
    f.is_completed,
    f.confirm_duration_sec,
    f.consult_duration_sec,
    f.canceled_date_key
  from f
  join week_map w
    on w.date_key = f.from_date_key
  {% if is_incremental() %}
    where w.week_start_date_key >= (
      select ifnull(max(week_start_date_key) - 100, 20000101)
      from {{ this }}
    )
  {% endif %}
)

select
  abs(farm_fingerprint(concat(
    cast(b.clinic_key as string), '|',
    cast(b.week_start_date_key as string)
  ))) as id,

  b.clinic_key,
  nc.clinic_name,
  b.week_start_date_key,

  countif(b.is_completed) as completed_booking_count,
  avg(b.confirm_duration_sec) as avg_confirm_duration_sec,
  avg(b.consult_duration_sec) as avg_consult_duration_sec,
  safe_divide(countif(b.canceled_date_key=10101), count(1)) as cancellation_rate

from base b
join name_cli nc
  on nc.clinic_key = b.clinic_key
group by 1,2,3,4
