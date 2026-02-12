{{ config(materialized='table') }}

select
  *,
  case
    when health_record_id is null then 'health_record_id_null'
    when clinic_key is null then 'clinic_key_null'
    when record_date_key is null then 'record_date_key_null'
    when age_group_key is null then 'age_group_key_null'
    else 'unknown'
  end as invalid_reason
from {{ ref('fact_health_record') }}
where
  health_record_id is null
  or clinic_key is null
  or record_date_key is null
  or age_group_key is null
