{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['clinic_key', 'doctor_key', 'room_id', 'date_key'],
    partition_by={"field": "date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key', 'doctor_key']
) }}

with f as (
  select
    clinic_key,
    doctor_key,
    room_id,
    prescription_date_key,
    price_included_vat,
    price
  from {{ source('platinum', 'fact_prescription_valid') }}
  {% if is_incremental() %}
    where prescription_date_key >= (
      select ifnull(max(date_key) - 31, 20000101)
      from {{ this }}
    )
  {% endif %}
),

name_cli as (
  select clinic_key, clinic_name
  from {{ source('platinum', 'dim_clinics') }}
),
name_doc as (
  select doctor_key, doctor_name
  from {{ source('platinum', 'dim_clinic_doctors') }}
)

select
  f.clinic_key,
  nc.clinic_name,
  f.doctor_key,
  nd.doctor_name,
  f.room_id,
  f.prescription_date_key as date_key,

  count(1) as prescription_count,

  -- total_value: prefer price_included_vat, else price
  sum(coalesce(f.price_included_vat, f.price)) as total_value,

  -- avg: average on the same chosen value
  avg(coalesce(f.price_included_vat, f.price)) as avg_price_effective

from f
join name_cli nc
  on nc.clinic_key = f.clinic_key
join name_doc nd
  on nd.doctor_key = f.doctor_key
group by 1,2,3,4,5,6
