{{ config(materialized='table') }}

with f as (
    select *
    from {{ source('platinum', 'fact_inventory_snapshot_valid') }}
),

lot as (
    select *
    from {{ source('platinum', 'dim_medicines_lot') }}
),

clinic as (
    select clinic_key, clinic_id, clinic_name
    from {{ source('platinum', 'dim_clinics') }}
),

medicine as (
    select medicine_key, medicine_id, medicine_name
    from {{ source('platinum', 'dim_medicines') }}
)

select
    c.clinic_name,
    m.medicine_name,
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
join clinic c
  on f.clinic_key = c.clinic_key
join medicine m
  on f.medicine_key = m.medicine_key
where f.current_quantity > 0