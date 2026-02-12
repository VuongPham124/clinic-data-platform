{{ config(materialized='table') }}

select
  *,
  case
    when disease_record_id is null then 'disease_record_id_null'
    when health_record_id is null then 'health_record_id_null'
    when disease_code is null then 'disease_code_null'
    else 'unknown'
  end as invalid_reason
from {{ ref('fact_disease_record') }}
where
  disease_record_id is null
  or health_record_id is null
  or disease_code is null
