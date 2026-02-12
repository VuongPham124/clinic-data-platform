{{ config(materialized='view') }}

select *
from {{ ref('fact_inventory_export') }}
where
  export_fact_id is not null
  and date_key is not null
  and clinic_key is not null
  and medicine_key is not null
  and lot_key is not null
