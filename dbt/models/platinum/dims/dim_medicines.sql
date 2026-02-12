{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as medicine_id,
    cast(`name` AS string) AS medicine_name,
    cast(`code` AS string) AS code,
    cast(`type` AS string) AS medicine_type,
    cast(`group` AS string) AS medicine_group,
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
      row_number() over (partition by medicine_id order by medicine_name) as rn
    from src
  )
  where rn = 1
)

select
  -- stable surrogate key
  abs(farm_fingerprint(cast(medicine_id as string))) as medicine_key,
  medicine_id,
  medicine_name,
  code,
  medicine_type,
  medicine_group,
  unit,
  manufacturer,
  vat
from dedup
where medicine_id is not null
