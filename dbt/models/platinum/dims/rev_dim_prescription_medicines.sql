{{ config(materialized='table') }}

select
  *,
  coalesce(safe_cast(price as numeric), 0) as clean_list_price,
  coalesce(safe_cast(unit_price as numeric), 0) as clean_unit_price,
  coalesce(safe_cast(vat_amount as numeric), 0) as clean_vat_amount,
  safe.parse_date('%Y-%m-%d', regexp_extract(cast(created_at as string), r'(\d{4}-\d{2}-\d{2})')) as booking_date
from {{ source('silver', 'prescription_medicines') }}
where deleted_at is null
