-- {{ config(materialized='table') }}

-- with f as (
--   select *
--   from {{ source('platinum', 'fact_prescription_valid') }}
-- )

-- select
--   clinic_key,
--   doctor_key,
--   room_id,
--   prescription_date_key as date_key,

--   count(1) as prescription_count,
--   sum(price_included_vat) as total_value,
--   avg(price_included_vat) as avg_price_included_vat

-- from f
-- group by 1,2,3,4

{{ config(materialized='table') }}

with f as (
  select *
  from {{ source('platinum', 'fact_prescription_valid') }}
)

select
  clinic_key,
  doctor_key,
  room_id,
  prescription_date_key as date_key,

  count(1) as prescription_count,

  -- total_value: prefer price_included_vat, else price
  sum(coalesce(price_included_vat, price)) as total_value,

  -- avg: average on the same chosen value
  avg(coalesce(price_included_vat, price)) as avg_price_effective

from f
group by 1,2,3,4