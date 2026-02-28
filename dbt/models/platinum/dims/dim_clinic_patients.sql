-- {{ config(materialized='table') }}

-- with src as (
--   select
--     cast(id as int64) as patient_id
--   from {{ source('silver', 'clinic_patients') }}
-- ),
-- dedup as (
--   select distinct patient_id
--   from src
--   where patient_id is not null
-- )

-- select
--   abs(farm_fingerprint(cast(patient_id as string))) as patient_key,
--   patient_id
-- from dedup
{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as patient_id,
    cast(user_id as int64) as patient_user_id,
    cast(clinic_id as int64) as clinic_patient_id
  from {{ source('silver', 'clinic_patients') }}
),
dedup as (
  select
    patient_id,
    any_value(patient_user_id) as patient_user_id,
    any_value(clinic_patient_id) as clinic_patient_id
  from src
  where patient_id is not null
  group by patient_id
)
select
  abs(farm_fingerprint(cast(patient_id as string))) as patient_key,
  patient_id,
  patient_user_id,
  clinic_patient_id
from dedup