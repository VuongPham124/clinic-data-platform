{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as medicine_import_detail_id,
    cast(lot_number as string) as lot_number,
    date(manufacturing_date) as manufacturing_date,
    date(expire_date) as expire_date,
    cast(import_price as numeric) as import_price,
    cast(`status` as string) as lot_status
  from {{ source('silver', 'medicine_import_details') }}
  where id is not null
)

select
  abs(farm_fingerprint(cast(medicine_import_detail_id as string))) as lot_key,
  medicine_import_detail_id,
  lot_number,
  manufacturing_date,
  expire_date,
  import_price,
  lot_status as status
from (
  select distinct *
  from src
)
