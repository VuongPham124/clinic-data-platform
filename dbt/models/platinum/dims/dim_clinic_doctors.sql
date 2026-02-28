{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as doctor_id,
    cast(full_name as string) as doctor_name,
    cast(clinic_user_id as int64) as clinic_doctor_id
  from {{ source('silver', 'clinic_doctors') }}
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (partition by doctor_id order by doctor_name) as rn
    from src
  )
  where rn = 1
)

select
  abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
  doctor_id,
  doctor_name,
  clinic_doctor_id
from dedup
where doctor_id is not null
