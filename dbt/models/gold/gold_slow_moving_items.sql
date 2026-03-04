{{ config(materialized='table') }}

with snapshot as (
    select
      clinic_key,
      medicine_key,
      lot_key,
      current_quantity
    from {{ source('platinum', 'fact_inventory_snapshot_valid') }}
),

export_f as (
    select
      clinic_key,
      lot_key,
      date_key
    from {{ source('platinum', 'fact_inventory_export_valid') }}
),

import_f as (
    select
        clinic_key,
        lot_key,
        date_key
    from {{ source('platinum', 'fact_inventory_import_valid') }}
),

d_date as (
    select
      date_key,
      year,
      month,
      full_date
    from {{ source('platinum', 'dim_date') }}
),

lot as (
    select
      lot_key,
      medicine_import_detail_id,
      expire_date
    from {{ source('platinum', 'dim_medicines_lot') }}
),

clinic as (
    select clinic_key, clinic_id, clinic_name
    from {{ source('platinum', 'dim_clinics') }}
),

medicine as (
    select medicine_key, medicine_id, medicine_name
    from {{ source('platinum', 'dim_medicines') }}
),

-- Các lô có phát sinh xuất trong 3 tháng gần nhất
lot_sold_last_3m as (
    select distinct
        e.clinic_key,
        e.lot_key
    from export_f e
    join d_date
      on e.date_key = d_date.date_key
    where date(d_date.year, d_date.month, 1)
          >= date_trunc(date_sub(current_date, interval 3 month), month)
      and date(d_date.year, d_date.month, 1)
          < date_trunc(current_date, month)
),

current_stock as (
    select *
    from snapshot
    where current_quantity > 0
),

-- Lấy ngày nhập kho từ fact_inventory_import
lot_import_date as (
    select
        i.clinic_key,
        i.lot_key,
        d_date.full_date as lot_start_date
    from import_f i
    join d_date
      on i.date_key = d_date.date_key
)

select
    c.clinic_name,
    m.medicine_name,
    lot.medicine_import_detail_id,

    -- Thông tin tồn
    s.current_quantity,

    -- Ngày nhập kho thực tế
    li.lot_start_date,

    -- Số ngày tồn kho tính từ ngày nhập
    date_diff(current_date, li.lot_start_date, day) as days_in_stock,

    lot.expire_date,

    case
        when l.lot_key is null then true
        else false
    end as is_slow_moving

from current_stock s

join lot
  on s.lot_key = lot.lot_key

join clinic c
  on s.clinic_key = c.clinic_key

join medicine m
  on s.medicine_key = m.medicine_key

left join lot_import_date li
  on s.clinic_key = li.clinic_key
 and s.lot_key = li.lot_key

left join lot_sold_last_3m l
  on s.clinic_key = l.clinic_key
 and s.lot_key = l.lot_key
