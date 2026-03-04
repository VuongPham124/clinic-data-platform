-- {{ config(materialized='table') }}

-- select
--   *,
--   case
--     when booking_id is null then 'booking_id_null'
--     when patient_key is null then 'patient_key_null'
--     when from_date_key is null then 'from_date_key_null'
--     else 'unknown'
--   end as invalid_reason
-- from {{ ref('fact_operational_clinic_bookings') }}
-- where
--   booking_id is null
--   or patient_key is null
--   or from_date_key is null
--   or created_date_key is null
--   or is_completed is null
--   or confirm_duration_sec is null
--   or consult_duration_sec is null


{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='booking_id',
    partition_by={"field": "created_date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key', 'doctor_key', 'patient_key']
) }}

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
{% if is_incremental() %}
  and created_date_key >= (
    select ifnull(max(created_date_key) - 31, 20000101)
    from {{ this }}
  )
{% endif %}
