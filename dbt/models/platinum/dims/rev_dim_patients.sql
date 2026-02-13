{{ config(materialized='view') }}

select *
from {{ source('silver', 'patients') }}
where deleted_at is null
