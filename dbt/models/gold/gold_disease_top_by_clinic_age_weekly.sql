{{ config(materialized='table') }}

with hr as (
  select *
  from {{ source('platinum', 'fact_health_record_valid') }}
),

dr as (
  select *
  from {{ source('platinum', 'fact_disease_record_valid') }}
),

d as (
  select
    date_key,
    full_date,
    cast(format_date('%Y%m%d', date_trunc(full_date, week(monday))) as int64) as week_start_date_key,
    week_of_year
  from {{ source('platinum', 'dim_date') }}
),

name_cli as (
  select clinic_key, clinic_name
  from {{ source('platinum', 'dim_clinics') }}
)

select
  hr.clinic_key,
  nc.clinic_name,
  d.week_start_date_key,
  hr.age_group_key,
  dr.disease_code,
  dr.disease_name,

  count(1) as total_occurrences,
  count(distinct hr.health_record_id) as unique_records

from dr
join hr
  on hr.health_record_id = cast(dr.health_record_id as string)
join d
  on d.date_key = hr.record_date_key
join name_cli nc
  on nc.clinic_key = hr.clinic_key

group by 1,2,3,4,5,6
