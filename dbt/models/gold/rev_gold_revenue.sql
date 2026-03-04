-- {{ config(materialized='table') }}

-- select
--   booking_id,
--   'OFFLINE' as source_type,
--   clinic_key,
--   doctor_key,
--   patient_key,
--   booking_date,
--   total_bill as gross_revenue,
--   amaz_revenue,
--   case
--     when payment_method = 'direct_payment' then (total_bill - commission_fee)
--     when payment_method in ('advance', 'full_payment') then paid_via_app
--     else null
--   end as partner_share,
--   service_amount as service_amount_only,
--   clean_prescription_amount as medicine_amount_total,
--   payment_channel,
--   is_revenue_eligible as is_valid_for_revenue
-- from {{ source('platinum', 'fact_operational_clinic_bookings_valid') }}
-- where is_revenue_eligible = true

-- union all

-- select
--   cast(id as string) as booking_id,
--   'ONLINE' as source_type,
--   null as clinic_id,
--   doctor_id,
--   patient_id,
--   booking_date,
--   clean_price as gross_revenue,
--   amaz_net_revenue as amaz_revenue,
--   clean_doctor_earning as partner_share,
--   clean_price as service_amount_only,
--   0 as medicine_amount_total,
--   'ONLINE_APP' as payment_channel,
--   is_valid_online_revenue as is_valid_for_revenue
-- from {{ source('platinum', 'rev_fact_bookings') }}
-- where is_valid_online_revenue = true


{{ config(materialized='table') }}

with d as (
  select
    date_key,
    full_date
  from {{ source('platinum', 'dim_date') }}
),

name_cli as (
  select clinic_key, clinic_name
  from {{ source('platinum', 'dim_clinics') }}
),

offline as (
  select
    f.booking_id,
    'OFFLINE' as source_type,

    f.clinic_key,
    nc.clinic_name,
    f.doctor_key,
    f.patient_key,

    d.full_date as booking_date,

    f.total_bill as gross_revenue,
    f.amaz_revenue,

    case
      when f.payment_method = 'direct_payment' then (f.total_bill - f.commission_fee)
      when f.payment_method in ('advance', 'full_payment') then f.paid_via_app
      else null
    end as partner_share,

    f.service_amount as service_amount_only,
    f.prescription_amount as medicine_amount_total,

    f.payment_channel,
    f.is_revenue_eligible as is_valid_for_revenue
  from {{ source('platinum', 'fact_operational_clinic_bookings_valid') }} f
  join d
    on d.date_key = f.from_date_key
  join name_cli nc
    on nc.clinic_key = f.clinic_key
  where f.is_revenue_eligible = true
),

online as (
  select
    CAST(o.id AS STRING) AS booking_id,
    'ONLINE' as source_type,

    -- online có thể không có clinic_key, để null
    cast(null as int64) as clinic_key,
    cast(null as string) as clinic_name,
    o.doctor_key,
    o.patient_key,

    d.full_date as booking_date,

    o.clean_price as gross_revenue,
    o.amaz_net_revenue as amaz_revenue,
    o.clean_doctor_earning as partner_share,
    o.clean_price as service_amount_only,
    cast(0 as numeric) as medicine_amount_total,
    'ONLINE_APP' as payment_channel,
    o.is_valid_online_revenue as is_valid_for_revenue
  from {{ source('platinum', 'rev_fact_bookings') }} o
  join d
    on d.date_key = o.booking_date_key
  where o.is_valid_online_revenue = true
)

select * from offline
union all
select * from online
