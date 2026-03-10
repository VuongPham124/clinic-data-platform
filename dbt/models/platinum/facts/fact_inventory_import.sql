{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='import_fact_id',
    partition_by={"field": "date_key", "data_type": "int64", "range": {"start": 20000101, "end": 21000101, "interval": 100}},
    cluster_by=['clinic_key', 'medicine_key']
) }}
with src_raw as (

    select
        cast(id as int64) as medicine_import_detail_id,
        cast(clinic_id as int64) as clinic_id,
        cast(medicine_id as int64) as medicine_id,
        safe_cast(created_at as timestamp) as created_ts,
        cast(total as int64) as quantity_imported,
        safe_cast(import_price as numeric) as import_price
    from {{ source('silver', 'medicine_import_details') }}
    where lower(`status`) = 'active'
    {% if is_incremental() %}
      and safe_cast(created_at as timestamp) >= (
        select timestamp(date_sub(parse_date('%Y%m%d', cast(ifnull(max(date_key), 20000101) as string)), interval 31 day))
        from {{ this }}
      )
    {% endif %}

),
src as (
    select * except(rn)
    from (
        select
            *,
            row_number() over (
                partition by medicine_import_detail_id
                order by created_ts desc
            ) as rn
        from src_raw
        where medicine_import_detail_id is not null
    )
    where rn = 1

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

    left join lots l
        on l.medicine_import_detail_id = s.medicine_import_detail_id
),
deduped as (
    select * except(rn)
    from (
        select
            *,
            row_number() over (
                partition by medicine_import_detail_id
                order by date_key desc, lot_key desc
            ) as rn
        from joined
    )
    where rn = 1
)

select
    cast(medicine_import_detail_id as string) as import_fact_id,
    *
from deduped
where medicine_import_detail_id is not null
