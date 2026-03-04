{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    partition_by={"field": "month_start_date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['doctor_key']
) }}

with f as (
  select
    doctor_key,
    booking_date_key
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
),

base as (
  select
    f.doctor_key,
    d.month_start_date_key
  from f
  join d
    on d.date_key = f.booking_date_key
  {% if is_incremental() %}
    where d.month_start_date_key >= (
      select ifnull(max(month_start_date_key) - 100, 20000101)
      from {{ this }}
    )
  {% endif %}
)

select
  abs(farm_fingerprint(concat(
    cast(b.doctor_key as string), '|',
    cast(b.month_start_date_key as string)
  ))) as id,

  b.doctor_key,
  nd.user_full_name,
  b.month_start_date_key,
  count(1) as number_booking

from base b
join name_doc nd
  on nd.doctor_key = b.doctor_key
group by 1,2,3,4
