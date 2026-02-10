{{ config(materialized='table') }}

with b as (
  select
    cast(id as int64) as booking_id,
    cast(clinic_id as int64) as clinic_id,
    cast(doctor_id as int64) as doctor_id,
    cast(patient_id as int64) as patient_id,

    safe_cast(created_at as timestamp) as created_ts,
    safe_cast(from_time as timestamp) as from_ts,
    safe_cast(confirmed_at as timestamp) as confirmed_ts,
    safe_cast(finished_at as timestamp) as finished_ts,
    safe_cast(canceled_at as timestamp) as canceled_ts
  from {{ source('silver', 'clinic_bookings') }}
),

bk as (
  select
    cast(booking_id as string) as booking_id,

    abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
    abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
    abs(farm_fingerprint(cast(patient_id as string))) as patient_key,

    cast(format_timestamp('%Y%m%d', created_ts) as int64) as created_date_key,
    cast(format_timestamp('%Y%m%d', from_ts) as int64) as from_date_key,
    cast(format_timestamp('%Y%m%d', confirmed_ts) as int64) as confirmed_date_key,
    cast(format_timestamp('%Y%m%d', finished_ts) as int64) as finished_date_key,
    cast(format_timestamp('%Y%m%d', canceled_ts) as int64) as canceled_date_key,

    (finished_ts is not null and canceled_ts is null) as is_completed,

    case
      when created_ts is not null and confirmed_ts is not null
        then timestamp_diff(confirmed_ts, created_ts, second)
      else null
    end as confirm_duration_sec,

    case
      when from_ts is not null and finished_ts is not null
        then timestamp_diff(finished_ts, from_ts, second)
      else null
    end as consult_duration_sec
  from b
  where booking_id is not null
)

select
  bk.*,
  -- room_key derived from clinic_key + doctor_key mapping
  r.room_id
from bk
left join {{ ref('dim_clinic_rooms') }} r
  on r.clinic_key = bk.clinic_key
 and r.doctor_key = bk.doctor_key
