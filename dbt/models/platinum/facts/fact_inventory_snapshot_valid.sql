{{ config(materialized='view') }}

select *
from {{ ref('fact_inventory_snapshot') }}
where
  snapshot_fact_id is not null
  and clinic_key is not null
  and medicine_key is not null
  and lot_key is not null
