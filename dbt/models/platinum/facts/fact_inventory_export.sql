{{ config(materialized='table') }}

with pmd as (

    select
        cast(id as int64) as prescription_medicine_detail_id,
        cast(clinic_id as int64) as clinic_id,
        cast(medicine_import_detail_id as int64) as medicine_import_detail_id,
        cast(prescription_medicine_id as int64) as prescription_medicine_id,
        cast(quantity as int64) as quantity_exported,
        safe_cast(created_at as timestamp) as created_ts
    from {{ source('silver', 'prescription_medicine_details') }}

),

pm as (

    select
        cast(id as int64) as prescription_medicine_id,
        cast(medicine_id as int64) as medicine_id
    from {{ source('silver', 'prescription_medicines') }}

),

base as (

    select
        pmd.prescription_medicine_detail_id,
        pmd.clinic_id,
        pm.medicine_id,
        pmd.medicine_import_detail_id,
        pmd.quantity_exported,
        pmd.created_ts
    from pmd
    left join pm
        on pmd.prescription_medicine_id = pm.prescription_medicine_id

),

joined as (

    select
        b.prescription_medicine_detail_id,

        d.date_key,
        c.clinic_key,
        m.medicine_key,
        l.lot_key,

        b.quantity_exported

    from base b

    left join {{ ref('dim_date') }} d
        on d.full_date = date(b.created_ts)

    left join {{ ref('dim_clinics') }} c
        on c.clinic_id = b.clinic_id

    left join {{ ref('dim_medicines') }} m
        on m.medicine_id = b.medicine_id

    left join {{ ref('dim_medicines_lot') }} l
        on l.medicine_import_detail_id = b.medicine_import_detail_id
)

select
    cast(prescription_medicine_detail_id as string) as export_fact_id,
    date_key,
    clinic_key,
    medicine_key,
    lot_key,
    quantity_exported
from joined
where prescription_medicine_detail_id is not null
