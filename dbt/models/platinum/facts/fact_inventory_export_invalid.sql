{{ config(materialized='table') }}

select
  *,
  case
    when export_fact_id is null then 'export_fact_id_null'
    when date_key is null then 'date_key_null'
    when clinic_key is null then 'clinic_key_null'
    when medicine_key is null then 'medicine_key_null'
    when lot_key is null then 'lot_key_null'
    else 'unknown'
  end as invalid_reason
from {{ ref('fact_inventory_export') }}
where
  export_fact_id is null
  or date_key is null
  or clinic_key is null
  or medicine_key is null
  or lot_key is null
