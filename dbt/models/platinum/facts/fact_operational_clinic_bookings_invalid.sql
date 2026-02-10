{{ config(materialized='table') }}

select
  *,
  case
    when booking_id is null then 'booking_id_null'
    when patient_key is null then 'patient_key_null'
    when from_date_key is null then 'from_date_key_null'
    else 'unknown'
  end as invalid_reason
from {{ ref('fact_operational_clinic_bookings') }}
where
  booking_id is null
  or patient_key is null
  or from_date_key is null
  or created_date_key is null
  or is_completed is null
  or confirm_duration_sec is null
  or consult_duration_sec is null
