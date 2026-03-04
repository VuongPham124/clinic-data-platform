{{ config(materialized='view') }}

with doctors as (
  select
    *,
    cast(id as int64) as doctor_id
  from {{ source('silver', 'doctors') }}
  where deleted_at is null
),

users as (
  select
    cast(id as int64) as user_id,
    full_name as user_full_name
  from {{ source('silver', 'public_users') }}
),

src as (
  select
    d.*,
    u.user_full_name
  from doctors d
  left join users u on d.user_id = u.user_id
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (partition by doctor_id order by doctor_id) as rn
    from src
  )
  where rn = 1
)

select
  abs(farm_fingerprint(cast(doctor_id as string))) as doctor_key,
  doctor_id,
  user_full_name,
  dedup.* except(doctor_id, user_full_name)
from dedup
where doctor_id is not null
