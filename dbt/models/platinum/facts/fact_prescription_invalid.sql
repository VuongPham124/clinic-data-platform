{{ config(materialized='table') }}

select
  *,
  case
    when prescription_id is null then 'prescription_id_null'
    when clinic_key is null then 'clinic_key_null'
    when doctor_key is null then 'doctor_key_null'
    when prescription_date_key is null then 'prescription_date_key_null'
    when price_included_vat is null then 'price_included_vat_null'
    else 'unknown'
  end as invalid_reason
from {{ ref('fact_prescription') }}
where
  prescription_id is null
  or clinic_key is null
  or doctor_key is null
  or prescription_date_key is null
  or price_included_vat is null
