{{ config(materialized='view') }}

with dr as (
  select
    cast(id as int64) as disease_record_id,
    cast(health_record_id as int64) as health_record_id,
    cast(code as string) as disease_code,
    `name` as disease_name
  from {{ source('silver', 'disease_records') }}
)

select
  dr.disease_record_id,
  dr.health_record_id,
  disease_code,
  disease_name
from dr
where disease_record_id is not null
