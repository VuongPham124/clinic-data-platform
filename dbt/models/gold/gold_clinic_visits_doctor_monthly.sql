{{ config(materialized='table') }}

with f as (
  select *
  from {{ source('platinum', 'fact_operational_clinic_bookings_valid') }}
),

d as (
  select
    date_key,
    month_start_date_key
  from {{ source('platinum', 'dim_date') }}
),

name_cli as (
  select clinic_key, clinic_name
  from {{ source('platinum', 'dim_clinics') }}
)

select
  abs(farm_fingerprint(concat(
    cast(f.clinic_key as string), '|',
    cast(d.month_start_date_key as string)
  ))) as id,

  f.clinic_key,
  nc.clinic_name,
  d.month_start_date_key,
  count(1) as number_booking

from f
join d
  on d.date_key = f.from_date_key
join name_cli nc
  on nc.clinic_key = f.clinic_key
group by 1,2,3,4
