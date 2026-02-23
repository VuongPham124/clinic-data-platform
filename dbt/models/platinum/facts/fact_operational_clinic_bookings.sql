-- {{ config(materialized='table') }}

-- with b as (
--   select
--     cast(id as int64) as booking_id,
--     cast(clinic_id as int64) as clinic_id,
--     cast(doctor_id as int64) as doctor_id,
--     cast(patient_id as int64) as patient_id,

--     safe_cast(created_at as timestamp) as created_ts,
--     safe_cast(from_time as timestamp) as from_ts,
--     safe_cast(confirmed_at as timestamp) as confirmed_ts,
--     safe_cast(finished_at as timestamp) as finished_ts,
--     safe_cast(canceled_at as timestamp) as canceled_ts
--   from {{ source('silver', 'clinic_bookings') }}
-- ),

-- bk as (
--   select
--     cast(booking_id as string) as booking_id,

--     abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
--     abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
--     abs(farm_fingerprint(cast(patient_id as string))) as patient_key,

--     cast(format_timestamp('%Y%m%d', created_ts) as int64) as created_date_key,
--     cast(format_timestamp('%Y%m%d', from_ts) as int64) as from_date_key,
--     cast(format_timestamp('%Y%m%d', confirmed_ts) as int64) as confirmed_date_key,
--     cast(format_timestamp('%Y%m%d', finished_ts) as int64) as finished_date_key,
--     cast(format_timestamp('%Y%m%d', canceled_ts) as int64) as canceled_date_key,

--     (finished_ts is not null and canceled_ts is null) as is_completed,

--     case
--       when created_ts is not null and confirmed_ts is not null
--         then timestamp_diff(confirmed_ts, created_ts, second)
--       else null
--     end as confirm_duration_sec,

--     case
--       when from_ts is not null and finished_ts is not null
--         then timestamp_diff(finished_ts, from_ts, second)
--       else null
--     end as consult_duration_sec
--   from b
--   where booking_id is not null
-- )

-- select
--   bk.*,
--   -- room_key derived from clinic_key + doctor_key mapping
--   r.room_id
-- from bk
-- left join {{ ref('dim_clinic_rooms') }} r
--   on r.clinic_key = bk.clinic_key
--  and r.doctor_key = bk.doctor_key


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
    safe_cast(canceled_at as timestamp) as canceled_ts,

    -- revenue-related (may exist depending on schema)
    cast(prescription_amount as numeric) as prescription_amount,
    cast(clinic_commission_fee as numeric) as clinic_commission_fee,
    cast(patient_discount_amount as numeric) as patient_discount_amount,
    cast(payment_method as string) as payment_method,
    cast(payment_status as string) as payment_status,
    cast(exam_status as string) as exam_status,
    cast(metadata as string) as metadata

  from {{ source('silver', 'clinic_bookings') }}
),

rev as (
  select
    *,
    -- service_amount from metadata: $.services.total_amount (string -> numeric)
    cast(coalesce(nullif(json_value(metadata, '$.services.total_amount'), ''), '0') as numeric) as service_amount,

    -- paid_amount from metadata: sum($.payment_data[].amount)
    coalesce((
      select sum(cast(json_value(item, '$.amount') as numeric))
      from unnest(json_query_array(metadata, '$.payment_data')) as item
    ), 0) as paid_amount
  from b 
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

    (finished_ts != TIMESTAMP `0001-01-01 00:00:00`) as is_completed,

    case
      when created_ts < confirmed_ts 
      then timestamp_diff(confirmed_ts, created_ts, second)
    end as confirm_duration_sec,

    case 
      when from_ts < finished_ts 
      then timestamp_diff(finished_ts, from_ts, second)
    end as consult_duration_sec,

    -- revenue fields
    service_amount,
    coalesce(prescription_amount, 0) as prescription_amount,
    coalesce(clinic_commission_fee, 0) as commission_fee,
    coalesce(patient_discount_amount, 0) as voucher_amount,
    paid_amount,

    (service_amount + coalesce(prescription_amount, 0)) as total_bill,
    cast(payment_method as string) as payment_method,


    case
      when payment_method in ('advance', 'full_payment') then 'OFFLINE_APP'
      when payment_method = 'direct_payment' then 'OFFLINE_CASH'
      else 'UNKNOWN'
    end as payment_channel,

    case when payment_method = 'direct_payment' then paid_amount else 0 end as clinic_cash_received,
    case when payment_method in ('advance', 'full_payment') then paid_amount else 0 end as paid_via_app,

    case
      when payment_method in ('advance', 'full_payment', 'direct_payment')
        then (coalesce(clinic_commission_fee,0) - coalesce(patient_discount_amount,0))
      else 0
    end as amaz_revenue,

    case
      when payment_method = 'direct_payment'
        then (service_amount + coalesce(prescription_amount,0) - coalesce(clinic_commission_fee,0))
      else null
    end as clinic_net_revenue,

    case
      when exam_status = 'finished' and payment_status = 'paid' then true
      else false
    end as is_revenue_eligible

  from rev
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
