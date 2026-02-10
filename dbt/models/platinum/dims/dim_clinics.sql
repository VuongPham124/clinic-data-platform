{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as clinic_id,
    cast(`name` AS string) AS clinic_name
  from {{ source('silver', 'clinics') }}
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (partition by clinic_id order by clinic_name) as rn
    from src
  )
  where rn = 1
)

select
  -- stable surrogate key
  abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
  clinic_id,
  clinic_name
from dedup
where clinic_id is not null
