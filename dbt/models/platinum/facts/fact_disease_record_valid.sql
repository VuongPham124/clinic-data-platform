{{ config(materialized='view') }}

select *
from {{ ref('fact_disease_record') }}
where
  disease_record_id is not null
  and health_record_id is not null
  and disease_code is not null
