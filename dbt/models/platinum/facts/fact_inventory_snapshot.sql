{{ config(materialized='table') }}

-- 1️⃣ Lấy total từ import detail
with imports as (

    select
        cast(id as int64) as medicine_import_detail_id,
        cast(clinic_id as int64) as clinic_id,
        cast(medicine_id as int64) as medicine_id,
        cast(total as int64) as quantity_imported,
        safe_cast(expire_date as date) as expire_date
    from {{ source('silver', 'medicine_import_details') }}

),

-- 2️⃣ Tổng số lượng đã export
exports as (

    select
        l.medicine_import_detail_id,
        sum(f.quantity_exported) as total_exported
    from {{ ref('fact_inventory_export') }} f
    join {{ ref('dim_medicines_lot') }} l
        on f.lot_key = l.lot_key
    group by l.medicine_import_detail_id

),

-- 3️⃣ Tính current_quantity
calculated as (

    select
        i.medicine_import_detail_id,
        i.clinic_id,
        i.medicine_id,
        i.expire_date,
        i.quantity_imported,
        coalesce(e.total_exported, 0) as total_exported,

        i.quantity_imported - coalesce(e.total_exported, 0) as current_quantity

    from imports i
    left join exports e
        on i.medicine_import_detail_id = e.medicine_import_detail_id
),

-- 4️⃣ Join dim
joined as (

    select
        c.medicine_import_detail_id,
        dc.clinic_key,
        dm.medicine_key,
        dl.lot_key,
        c.current_quantity,
        date_diff(c.expire_date, current_date(), day) as days_to_expire

    from calculated c

    left join {{ ref('dim_clinics') }} as dc
        on dc.clinic_id = c.clinic_id

    left join {{ ref('dim_medicines') }} as dm
        on dm.medicine_id = c.medicine_id

    left join {{ ref('dim_medicines_lot') }} as dl
        on dl.medicine_import_detail_id = c.medicine_import_detail_id
)

select
    cast(medicine_import_detail_id as string) as snapshot_fact_id,
    clinic_key,
    medicine_key,
    lot_key,
    current_quantity,
    days_to_expire
from joined
