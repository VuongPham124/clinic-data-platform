-- {{ config(materialized='view') }}

-- select *
-- from {{ ref('fact_prescription') }}
-- where
--   prescription_id is not null
--   and clinic_key is not null
--   and doctor_key is not null
--   and prescription_date_key is not null
--   and price_included_vat is not null


{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='prescription_id',
    partition_by={"field": "prescription_date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 1}},
    cluster_by=['clinic_key', 'doctor_key']
) }}

select *
from {{ ref('fact_prescription') }}
where
  prescription_id is not null
  and clinic_key is not null
  and doctor_key is not null
  and prescription_date_key is not null
{% if is_incremental() %}
  and prescription_date_key >= (
    select ifnull(max(prescription_date_key) - 31, 20000101)
    from {{ this }}
  )
{% endif %}
