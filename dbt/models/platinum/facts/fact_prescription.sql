-- {{ config(materialized='table') }}

-- with p as (
--   select
--     cast(id as int64) as prescription_id,
--     cast(clinic_id as int64) as clinic_id,
--     cast(doctor_id as int64) as doctor_id,
    

--     safe_cast(created_at as timestamp) as prescription_ts,
--     cast(price as numeric) as price,
--     cast(price_included_vat as numeric) as price_included_vat,
--     cast(vat_amount as numeric) as vat_amount
--   from {{ source('silver', 'prescriptions') }}
-- ),

-- base as (
--   select
--     cast(prescription_id as string) as prescription_id,

--     abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
--     abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
--     -- abs(farm_fingerprint(cast(room_id as string))) as room_key,

--     cast(format_timestamp('%Y%m%d', prescription_ts) as int64) as prescription_date_key,

--     price,
--     price_included_vat,
--     vat_amount
--   from p
-- )

-- -- select *
-- -- from base
-- -- where prescription_id is not null

-- select
--   base.*,
--   -- room_key derived from clinic_key + doctor_key mapping
--   r.room_id
-- from base
-- left join {{ ref('dim_clinic_rooms') }} r
--   on r.clinic_key = base.clinic_key
--  and r.doctor_key = base.doctor_key
-- where base.prescription_id is not null


{{ config(materialized='table') }}

with p as (
  select
    cast(id as int64) as prescription_id,
    cast(clinic_id as int64) as clinic_id,
    cast(doctor_id as int64) as doctor_id,

    safe_cast(created_at as timestamp) as prescription_ts,
    cast(price as numeric) as price,
    cast(price_included_vat as numeric) as price_included_vat,
    cast(vat_amount as numeric) as vat_amount
  from {{ source('silver', 'prescriptions') }}
),

base as (
  select
    cast(prescription_id as string) as prescription_id,

    abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
    abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,

    cast(format_timestamp('%Y%m%d', prescription_ts) as int64) as prescription_date_key,

    price,
    price_included_vat,
    vat_amount
  from p
)

select
  base.*,
  r.room_id
from base
left join {{ ref('dim_clinic_rooms') }} r
  on r.clinic_key = base.clinic_key
 and r.doctor_key = base.doctor_key
where base.prescription_id is not null

