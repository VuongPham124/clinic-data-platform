{{ config(materialized='table') }}

select *
from {{ ref('fact_health_record') }}
where
  health_record_id is not null
  and clinic_key is not null
  and record_date_key is not null
  and age_group_key is not null
