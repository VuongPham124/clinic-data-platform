{{ config(materialized='view') }}

with dr as (
  select
    cast(id as int64) as disease_record_id,
    cast(health_record_id as int64) as health_record_id,
    cast(code as string) as disease_code
  from {{ source('silver', 'disease_records') }}
)

select
  cast(disease_record_id as string) as disease_record_id,
  cast(health_record_id as string) as health_record_id,
  disease_code
from dr
where disease_record_id is not null
