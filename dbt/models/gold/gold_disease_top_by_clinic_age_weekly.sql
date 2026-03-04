{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['clinic_key', 'week_start_date_key', 'age_group_key', 'disease_code'],
    partition_by={"field": "week_start_date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key', 'age_group_key']
) }}

with hr as (
  select
    health_record_id,
    clinic_key,
    age_group_key,
    record_date_key
  from {{ source('platinum', 'fact_health_record_valid') }}
),

dr as (
  select
    health_record_id,
    disease_code,
    disease_name
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
),

base as (
  select
    hr.clinic_key,
    d.week_start_date_key,
    hr.age_group_key,
    dr.disease_code,
    dr.disease_name,
    hr.health_record_id
  from dr
  join hr
    on hr.health_record_id = cast(dr.health_record_id as string)
  join d
    on d.date_key = hr.record_date_key
  {% if is_incremental() %}
    where d.week_start_date_key >= (
      select ifnull(max(week_start_date_key) - 100, 20000101)
      from {{ this }}
    )
  {% endif %}
)

select
  b.clinic_key,
  nc.clinic_name,
  b.week_start_date_key,
  b.age_group_key,
  b.disease_code,
  b.disease_name,

  count(1) as total_occurrences,
  count(distinct b.health_record_id) as unique_records

from base b
join name_cli nc
  on nc.clinic_key = b.clinic_key

group by 1,2,3,4,5,6
