{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='export_fact_id',
    partition_by={"field": "date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 1}},
    cluster_by=['clinic_key', 'medicine_key']
) }}

select *
from {{ ref('fact_inventory_export') }}
where
  export_fact_id is not null
  and date_key is not null
  and clinic_key is not null
  and medicine_key is not null
  and lot_key is not null
{% if is_incremental() %}
  and date_key >= (
    select ifnull(max(date_key) - 31, 20000101)
    from {{ this }}
  )
{% endif %}
