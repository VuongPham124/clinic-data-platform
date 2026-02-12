{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as medicine_import_detail_id,
    cast(lot_number as string) as lot_number,
    safe_cast(expire_date as timestamp) as expire_date,
    cast(import_price as numeric) as import_price,
    cast(status as string) as status,
  from {{ source('silver', 'medicine_import_details') }}
),
dedup as (
  select distinct medicine_import_detail_id
  from src
  where medicine_import_detail_id is not null
)

select
  abs(farm_fingerprint(cast(medicine_import_detail_id as string))) as lot_key,
  medicine_import_detail_id,
  lot_number,
  expire_date,
  import_price,
  status
from dedup
