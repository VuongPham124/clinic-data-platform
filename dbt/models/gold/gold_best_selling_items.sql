{{ config(materialized='table') }}

with f as (
    select *
    from {{ source('platinum', 'fact_inventory_export_valid') }}
),

d as (
    select *
    from {{ source('platinum', 'dim_date') }}
),

clinic as (
    select clinic_key, clinic_id, clinic_name
    from {{ source('platinum', 'dim_clinics') }}
),

medicine as (
    select medicine_key, medicine_id, medicine_name
    from {{ source('platinum', 'dim_medicines') }}
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
    c.clinic_name,
    m.medicine_name,
    ms.year,
    ms.month,
    ms.total_quantity_exported,

    rank() over (
        partition by ms.clinic_key, ms.year, ms.month
        order by ms.total_quantity_exported desc
    ) as rank

from monthly_sales ms
join clinic c
  on ms.clinic_key = c.clinic_key
join medicine m
  on ms.medicine_key = m.medicine_key