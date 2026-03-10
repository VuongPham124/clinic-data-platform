{{ config(materialized='view') }}

with imports as (

    select
        cast(id as int64) as medicine_import_detail_id,
        cast(clinic_id as int64) as clinic_id,
        cast(medicine_id as int64) as medicine_id,
        cast(total as int64) as quantity_imported,
        safe_cast(import_price as numeric) as import_price,
        date(expire_date) as expire_date
    from {{ source('silver', 'medicine_import_details') }}
    where lower(`status`) = 'active'

),

exports as (

    select
        l.medicine_import_detail_id,
        sum(f.quantity_exported) as total_exported
    from {{ ref('fact_inventory_export') }} f
    join {{ ref('dim_medicines_lot') }} l
        on f.lot_key = l.lot_key
    group by l.medicine_import_detail_id

),

calculated as (

    select
        i.medicine_import_detail_id,
        i.clinic_id,
        i.medicine_id,
        i.expire_date,
        i.quantity_imported,
        i.import_price,
        coalesce(e.total_exported, 0) as total_exported,
        i.quantity_imported - coalesce(e.total_exported, 0) as current_quantity

    from imports i
    left join exports e
        on i.medicine_import_detail_id = e.medicine_import_detail_id
),

lots as (

    select
        medicine_import_detail_id,
        lot_key
    from (
        select
            medicine_import_detail_id,
            lot_key,
            row_number() over (
                partition by medicine_import_detail_id
                order by lot_key
            ) as rn
        from {{ ref('dim_medicines_lot') }}
        where medicine_import_detail_id is not null
    )
    where rn = 1
),

joined as (

    select
        c.medicine_import_detail_id,
        dc.clinic_key,
        dm.medicine_key,
        dl.lot_key,
        c.current_quantity,
        c.current_quantity * c.import_price as current_inventory_value,
        date_diff(c.expire_date, current_date(), day) as days_to_expire

    from calculated c

    left join {{ ref('dim_clinics') }} as dc
        on dc.admin_user_id = c.clinic_id

    left join {{ ref('dim_medicines') }} as dm
        on dm.medicine_id = c.medicine_id

    left join lots as dl
        on dl.medicine_import_detail_id = c.medicine_import_detail_id
),

deduped as (

    select * except(rn)
    from (
        select
            *,
            row_number() over (
                partition by medicine_import_detail_id
                order by lot_key desc
            ) as rn
        from joined
    )
    where rn = 1
)

select
    cast(medicine_import_detail_id as string) as snapshot_fact_id,
    clinic_key,
    medicine_key,
    lot_key,
    current_quantity,
    current_inventory_value,
    days_to_expire
from deduped
where current_quantity > 0
