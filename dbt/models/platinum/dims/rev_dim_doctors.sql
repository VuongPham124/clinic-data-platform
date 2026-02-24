{{ config(materialized='view') }}

with src as (
  select
    *,
    cast(id as int64) as doctor_id
  from {{ source('silver', 'doctors') }}
  where deleted_at is null
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (partition by doctor_id order by doctor_id) as rn
    from src
  )
  where rn = 1
)

select
  abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
  doctor_id,
  dedup.* except(doctor_id)
from dedup
where doctor_id is not null
