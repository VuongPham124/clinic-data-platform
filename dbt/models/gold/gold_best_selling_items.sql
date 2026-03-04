{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['clinic_name', 'medicine_name', 'year', 'month'],
    cluster_by=['year', 'month']
) }}

with f as (
    select
      clinic_key,
      medicine_key,
      date_key,
      quantity_exported
    from {{ source('platinum', 'fact_inventory_export_valid') }}
),

d as (
    select
      date_key,
      year,
      month
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
    {% if is_incremental() %}
      where cast(concat(cast(d.year as string), lpad(cast(d.month as string), 2, '0')) as int64) >= (
        select ifnull(max(year * 100 + month) - 1, 200001)
        from {{ this }}
      )
    {% endif %}
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
