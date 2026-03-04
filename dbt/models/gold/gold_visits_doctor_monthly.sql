gold_visits_doctor_monthly{{ config(materialized='table') }}

with f as (
  select *
  from {{ source('platinum', 'rev_fact_bookings') }}
),

d as (
  select
    date_key,
    month_start_date_key
  from {{ source('platinum', 'dim_date') }}
),

name_doc as (
  select doctor_key, user_full_name
  from {{ source('platinum', 'rev_dim_doctors') }}
)

select
  abs(farm_fingerprint(concat(
    cast(f.doctor_key as string), '|',
    cast(d.month_start_date_key as string)
  ))) as id,

  f.doctor_key,
  nd.user_full_name,
  d.month_start_date_key,
  count(1) as number_booking

from f
join d
  on d.date_key = f.booking_date_key
join name_doc nd
  on nd.doctor_key = f.doctor_key
group by 1,2,3,4
