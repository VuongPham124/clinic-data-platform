-- {{ config(materialized='table') }}

-- with src as (
--   select
--     cast(id as int64) as room_id,
--     cast(clinic_id as int64) as clinic_id,
--     cast(doctor_id as int64) as doctor_id,
--     cast(room_name as string) as room_name
--   from {{ source('silver', 'clinic_rooms') }}
-- ),
-- dedup as (
--   select * except(rn)
--   from (
--     select
--       *,
--       row_number() over (partition by room_id order by room_name) as rn
--     from src
--   )
--   where rn = 1
-- )

-- select
--   abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
--   abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
--   room_id,
--   room_name
-- from dedup
-- where room_id is not null
--   and clinic_id is not null
--   and doctor_id is not null

{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64) as room_id,
    cast(clinic_id as int64) as clinic_id,
    cast(doctor_id as int64) as doctor_id,
    cast(room_name as string) as room_name
  from {{ source('silver', 'clinic_rooms') }}
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (partition by room_id order by room_name) as rn
    from src
  )
  where rn = 1
)

select
  abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
  abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
  room_id,
  room_name
from dedup
where room_id is not null
  and clinic_id is not null
  and doctor_id is not null
