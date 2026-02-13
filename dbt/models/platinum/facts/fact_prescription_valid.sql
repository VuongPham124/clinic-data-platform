{{ config(materialized='view') }}

select *
from {{ ref('fact_prescription') }}
where
  prescription_id is not null
  and clinic_key is not null
  and doctor_key is not null
  and prescription_date_key is not null
  and price_included_vat is not null
