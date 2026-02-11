{{ config(materialized='table') }}

with hr as (
  select
    cast(id as int64) as health_record_id,
    cast(clinic_id as int64) as clinic_id,
    safe_cast(created_at as timestamp) as record_ts,
    cast(patient_age as int64) as patient_age
  from {{ source('silver', 'health_records') }}
),

base as (
  select
    cast(health_record_id as string) as health_record_id,
    abs(farm_fingerprint(cast(clinic_id as string))) as clinic_key,
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
)

select *
from base
where health_record_id is not null
