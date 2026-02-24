{{ config(materialized='view') }}

with src as (
  select
    *,
    cast(id as int64) as patient_id
  from {{ source('silver', 'patients') }}
  where deleted_at is null
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (partition by patient_id order by patient_id) as rn
    from src
  )
  where rn = 1
)

select
  abs(farm_fingerprint(cast(patient_id as string))) as patient_key,
  patient_id,
  dedup.* except(patient_id)
from dedup
where patient_id is not null
