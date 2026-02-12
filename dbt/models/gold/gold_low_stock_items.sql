{{ config(materialized='table') }}

with snapshot as (
    select *
    from {{ source('platinum', 'fact_inventory_snapshot_valid') }}
),

export_f as (
    select *
    from {{ source('platinum', 'fact_inventory_export_valid') }}
),

d as (
    select *
    from {{ source('platinum', 'dim_date') }}
),

sales_prev_month as (
    select
        e.clinic_key,
        e.medicine_key,
        d.year,
        d.month,
        sum(e.quantity_exported) as items_sold
    from export_f e
    join d
      on e.date_key = d.date_key
    group by 1,2,3,4
),

current_month as (
    select
        extract(year from current_date) as year,
        extract(month from current_date) as month
),

prev_month_sales as (
    select
        s.clinic_key,
        s.medicine_key,
        s.items_sold
    from sales_prev_month s
    join current_month c
      on (s.year = c.year and s.month = c.month - 1)
         or (c.month = 1 and s.year = c.year - 1 and s.month = 12)
)

select
    s.clinic_key,
    s.medicine_key,
    s.lot_key,
    s.current_quantity,
    coalesce(p.items_sold, 0) as items_sold_prev_month,

    case
        when s.current_quantity < coalesce(p.items_sold, 0)
        then true
        else false
    end as is_low_stock

from snapshot s
left join prev_month_sales p
  on s.clinic_key = p.clinic_key
 and s.medicine_key = p.medicine_key
where s.current_quantity > 0
