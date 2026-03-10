{{ config(materialized='view') }}

with hr_raw as (
  select
    cast(id as int64) as health_record_id,
    cast(clinic_id as int64) as clinic_id,
    safe_cast(created_at as timestamp) as record_ts,
    cast(patient_age as int64) as patient_age
  from {{ source('silver', 'health_records') }}
),

hr as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by health_record_id
        order by record_ts desc
      ) as rn
    from hr_raw
    where health_record_id is not null
  )
  where rn = 1
),

clinics as (
  select
    admin_user_id,
    clinic_key
  from (
    select
      admin_user_id,
      clinic_key,
      row_number() over (
        partition by admin_user_id
        order by clinic_key
      ) as rn
    from {{ ref('dim_clinics') }}
    where admin_user_id is not null
  )
  where rn = 1
),

base as (
  select
    cast(health_record_id as string) as health_record_id,
    dc.clinic_key,
    cast(format_timestamp('%Y%m%d', record_ts) as int64) as record_date_key,

    -- age_group_key: bucket hóa đơn giản (bạn có thể thay bằng dim_age_group join)
    case
      when patient_age is null then null
      when patient_age < 6 then 1
      when patient_age between 6 and 17 then 2
      when patient_age between 18 and 39 then 3
      when patient_age between 40 and 59 then 4
      else 5
    end as age_group_key
  from hr
  left join clinics as dc
    on dc.admin_user_id = hr.clinic_id
)

select *
from base
where health_record_id is not null
