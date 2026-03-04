{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    partition_by={"field": "month_start_date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key']
) }}

with f as (
  select
    clinic_key,
    from_date_key
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
),

base as (
  select
    f.clinic_key,
    d.month_start_date_key
  from f
  join d
    on d.date_key = f.from_date_key
  {% if is_incremental() %}
    where d.month_start_date_key >= (
      select ifnull(max(month_start_date_key) - 100, 20000101)
      from {{ this }}
    )
  {% endif %}
)

select
  abs(farm_fingerprint(concat(
    cast(b.clinic_key as string), '|',
    cast(b.month_start_date_key as string)
  ))) as id,

  b.clinic_key,
  nc.clinic_name,
  b.month_start_date_key,
  count(1) as number_booking

from base b
join name_cli nc
  on nc.clinic_key = b.clinic_key
group by 1,2,3,4
