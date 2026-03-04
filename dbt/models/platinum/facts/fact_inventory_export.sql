{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='export_fact_id',
    partition_by={"field": "date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key', 'medicine_key']
) }}

with pmd as (

    select
        cast(id as int64) as prescription_medicine_detail_id,
        cast(clinic_id as int64) as clinic_id,
        cast(medicine_import_detail_id as int64) as medicine_import_detail_id,
        cast(prescription_medicine_id as int64) as prescription_medicine_id,
        cast(quantity as int64) as quantity_exported,
        safe_cast(created_at as timestamp) as created_ts
    from {{ source('silver', 'prescription_medicine_details') }}
    {% if is_incremental() %}
      where safe_cast(created_at as timestamp) >= (
        select timestamp(date_sub(parse_date('%Y%m%d', cast(ifnull(max(date_key), 20000101) as string)), interval 31 day))
        from {{ this }}
      )
    {% endif %}

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
        on c.admin_user_id = b.clinic_id

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
