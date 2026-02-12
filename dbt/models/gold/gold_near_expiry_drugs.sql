{{ config(materialized='table') }}

with f as (
    select *
    from {{ source('platinum', 'fact_inventory_snapshot_valid') }}
),

lot as (
    select *
    from {{ source('platinum', 'dim_medicine_lot') }}
)

select
    f.clinic_key,
    f.medicine_key,
    f.lot_key,
    lot.medicine_import_detail_id,

    lot.expire_date,
    f.current_quantity,
    f.days_to_expire,

    case
        when f.days_to_expire < 30 then 'red'
        when f.days_to_expire between 30 and 90 then 'yellow'
        else 'green'
    end as urgent_level

from f
join lot
  on f.lot_key = lot.lot_key
where f.current_quantity > 0
