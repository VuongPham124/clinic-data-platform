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

{{ config(materialized='view') }}

select *
from {{ ref('fact_operational_clinic_bookings') }}
where
  booking_id is not null
  and patient_key is not null
  and from_date_key is not null
  and created_date_key is not null
  -- duration chỉ bắt buộc nếu completed
  and (
    is_completed = false
    or (confirm_duration_sec is not null and consult_duration_sec is not null)
  )