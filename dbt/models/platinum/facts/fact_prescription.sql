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

--     dc.clinic_key,
--     dd.doctor_key,

--     cast(format_timestamp('%Y%m%d', prescription_ts) as int64) as prescription_date_key,

--     price,
--     price_included_vat,
--     vat_amount
--   from p
--   left join {{ ref('dim_clinics') }} as dc
--     on dc.admin_user_id = p.clinic_id
--   left join {{ ref('dim_clinic_doctors') }} as dd
--     on dd.doctor_id = p.doctor_id
-- )

-- select
--   base.*,
--   r.room_id
-- from base
-- left join {{ ref('dim_clinic_rooms') }} as r
--   on r.clinic_key = base.clinic_key
--  and r.doctor_key = base.doctor_key
-- where base.prescription_id is not null

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='prescription_id',
    partition_by={"field": "prescription_date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key', 'doctor_key']
) }}

with p_raw as (
  select
    cast(id as int64) as prescription_id,
    cast(clinic_id as int64) as clinic_id,
    cast(doctor_id as int64) as doctor_id,

    safe_cast(created_at as timestamp) as prescription_ts,
    cast(price as numeric) as price,
    cast(price_included_vat as numeric) as price_included_vat,
    cast(vat_amount as numeric) as vat_amount
  from {{ source('silver', 'prescriptions') }}
  {% if is_incremental() %}
    where safe_cast(created_at as timestamp) >= (
      select timestamp(date_sub(parse_date('%Y%m%d', cast(ifnull(max(prescription_date_key), 20000101) as string)), interval 31 day))
      from {{ this }}
    )
  {% endif %}
),
p as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by prescription_id
        order by prescription_ts desc
      ) as rn
    from p_raw
    where prescription_id is not null
  )
  where rn = 1
),

base as (
  select
    cast(prescription_id as string) as prescription_id,

    dc.clinic_key,
    dd.doctor_key,

    cast(format_timestamp('%Y%m%d', prescription_ts) as int64) as prescription_date_key,

    price,
    price_included_vat,
    vat_amount
  from p
  left join {{ ref('dim_clinics') }} as dc
    on dc.admin_user_id = p.clinic_id
  left join {{ ref('dim_clinic_doctors') }} as dd
    on dd.clinic_doctor_id = p.doctor_id
),
rooms as (
  select
    clinic_key,
    doctor_key,
    room_id
  from (
    select
      clinic_key,
      doctor_key,
      room_id,
      row_number() over (
        partition by clinic_key, doctor_key
        order by room_id
      ) as rn
    from {{ ref('dim_clinic_rooms') }}
  )
  where rn = 1
),
final as (
  select
    base.*,
    r.room_id
  from base
  left join rooms as r
    on r.clinic_key = base.clinic_key
   and r.doctor_key = base.doctor_key
),
deduped as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by prescription_id
        order by prescription_date_key desc, room_id desc
      ) as rn
    from final
  )
  where rn = 1
)

select
  *
from deduped
where prescription_id is not null

