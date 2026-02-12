{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as medicine_id,
    cast(`name` AS string) AS medicine_name,
    cast(`code` AS string) AS code,
    cast(`type` AS boolean) AS type,
    cast(`group` AS string) AS group,
    cast(`unit` AS string) AS unit,
    cast(`manufacturer` AS string) AS manufacturer,
    cast(`vat` AS string) AS vat,
  from {{ source('silver', 'medicines') }}
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
  clinic_name,
  clinic_address,
  is_active,
  open_time,
  close_time
from dedup
where clinic_id is not null
