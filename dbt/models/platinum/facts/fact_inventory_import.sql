{{ config(materialized='view') }}

with src as (

    select
        cast(id as int64) as medicine_import_detail_id,
        cast(clinic_id as int64) as clinic_id,
        cast(medicine_id as int64) as medicine_id,
        safe_cast(created_at as timestamp) as created_ts,
        cast(total as int64) as quantity_imported,
        safe_cast(import_price as numeric) as import_price
    from {{ source('silver', 'medicine_import_details') }}
    where lower(`status`) = 'active'

),

joined as (

    select
        s.medicine_import_detail_id,
        d.date_key,
        c.clinic_key,
        m.medicine_key,
        l.lot_key,
        s.quantity_imported,
        s.quantity_imported * s.import_price as initial_import_value

    from src s

    left join {{ ref('dim_date') }} d
        on d.full_date = date(s.created_ts)

    left join {{ ref('dim_clinics') }} c
        on c.admin_user_id = s.clinic_id

    left join {{ ref('dim_medicines') }} m
        on m.medicine_id = s.medicine_id

    left join {{ ref('dim_medicines_lot') }} l
        on l.medicine_import_detail_id = s.medicine_import_detail_id
)

select
    cast(medicine_import_detail_id as string) as import_fact_id,
    *
from joined
where medicine_import_detail_id is not null
