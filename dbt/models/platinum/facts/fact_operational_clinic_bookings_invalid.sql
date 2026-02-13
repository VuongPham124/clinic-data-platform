-- {{ config(materialized='view') }}

-- select *
-- from {{ ref('fact_operational_clinic_bookings') }}
-- where
--   booking_id is not null
--   and patient_key is not null
--   and from_date_key is not null
--   and created_date_key is not null
--   and is_completed is not null
--   and confirm_duration_sec is not null
--   and consult_duration_sec is not null


{{ config(materialized='table') }}

select
  *,
  case
    when booking_id is null then 'booking_id_null'
    when patient_key is null then 'patient_key_null'
    when from_date_key is null then 'from_date_key_null'
    when created_date_key is null then 'created_date_key_null'
    when is_completed = true and confirm_duration_sec is null then 'confirm_duration_sec_null_completed'
    when is_completed = true and consult_duration_sec is null then 'consult_duration_sec_null_completed'
    else 'unknown'
  end as invalid_reason
from {{ ref('fact_operational_clinic_bookings') }}
where
  booking_id is null
  or patient_key is null
  or from_date_key is null
  or created_date_key is null
  or (is_completed = true and (confirm_duration_sec is null or consult_duration_sec is null))
