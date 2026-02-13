{{ config(materialized='table') }}

with f as (
    select *
    from {{ source('platinum', 'fact_inventory_export_valid') }}
),

d as (
    select *
    from {{ source('platinum', 'dim_date') }}
),

monthly_sales as (
    select
        f.clinic_key,
        f.medicine_key,
        d.year,
        d.month,
        sum(f.quantity_exported) as total_quantity_exported
    from f
    join d
      on f.date_key = d.date_key
    group by 1,2,3,4
)

select
    clinic_key,
    medicine_key,
    year,
    month,
    total_quantity_exported,

    rank() over (
        partition by clinic_key, year, month
        order by total_quantity_exported desc
    ) as rank

from monthly_sales
