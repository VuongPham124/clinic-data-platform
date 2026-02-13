{{ config(materialized='view') }}

select *
from {{ source('silver', 'doctors') }}
where deleted_at is null
