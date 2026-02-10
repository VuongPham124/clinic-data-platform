{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as patient_id
  from {{ source('silver', 'clinic_patients') }}
),
dedup as (
  select distinct patient_id
  from src
  where patient_id is not null
)

select
  abs(farm_fingerprint(cast(patient_id as string))) as patient_key,
  patient_id
from dedup
