{{ config(materialized='table') }}

with f as (
  select
    clinic_key,
    patient_key,
    from_date_key
  from {{ source('platinum', 'fact_operational_clinic_bookings_valid') }}
  where patient_key is not null
),

d as (
  select
    date_key,
    month_start_date_key
  from {{ source('platinum', 'dim_date') }}
),

visits as (
  -- one row per (clinic, patient, month) that had at least one booking
  select
    f.clinic_key,
    f.patient_key,
    d.month_start_date_key
  from f
  join d
    on d.date_key = f.from_date_key
  group by 1,2,3
),

first_month as (
  select
    clinic_key,
    patient_key,
    min(month_start_date_key) as first_visit_month_start_date_key
  from visits
  group by 1,2
),

name_cli as (
  select clinic_key, clinic_name
  from {{ source('platinum', 'dim_clinics') }}
),

monthly as (
  select
    v.clinic_key,
    nc.clinic_name,
    v.month_start_date_key,
    countif(fm.first_visit_month_start_date_key = v.month_start_date_key) as new_visits,
    countif(fm.first_visit_month_start_date_key < v.month_start_date_key) as returning_visits
  from visits v
  join first_month fm
    on fm.clinic_key = v.clinic_key
   and fm.patient_key = v.patient_key
  join name_cli nc
    on nc.clinic_key = v.clinic_key
  group by 1,2,3
)

select
  abs(farm_fingerprint(concat(
    cast(clinic_key as string), '|',
    cast(month_start_date_key as string)
  ))) as id,

  clinic_key,
  clinic_name,
  month_start_date_key,
  new_visits,
  returning_visits,
  safe_divide(returning_visits, nullif(new_visits + returning_visits, 0)) as retention_rate

from monthly
