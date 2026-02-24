{{ config(materialized='table') }}

with base_data as (
  select
    b.*,
    CAST(FORMAT_TIMESTAMP('%Y%m%d', CAST(from_time AS TIMESTAMP)) AS INT64) AS booking_date_key,
    cast(coalesce(nullif(cast(b.patient_paying_amount as string), ''), '0') as numeric) as clean_patient_paying,
    cast(coalesce(nullif(cast(b.doctor_earning_amount as string), ''), '0') as numeric) as clean_doctor_earning,
    cast(coalesce(nullif(cast(b.patient_discount_amount as string), ''), '0') as numeric) as clean_patient_discount,
    cast(coalesce(nullif(cast(b.price as string), ''), '0') as numeric) as clean_price,
    cast(coalesce(nullif(cast(b.service_vat as string), ''), '0') as numeric) as clean_service_vat

  from {{ source('silver', 'bookings') }} b
  where b.deleted_at is null
    and regexp_contains(cast(b.price as string), r'^[0-9.]+$')
), joined as (
  select
    base_data.*,
    rd.doctor_key,
    rp.patient_key
  from base_data
  left join {{ ref('rev_dim_doctors') }} as rd
    on rd.doctor_id = base_data.doctor_id
  left join {{ ref('rev_dim_patients') }} as rp
    on rp.patient_id = base_data.patient_id
)

select
  joined.*,

  (clean_patient_paying - clean_doctor_earning) as amaz_net_revenue,

  case
    when status = 5 and clean_price > 0 then true
    else false
  end as is_valid_online_revenue,

  case
    when clean_patient_paying = 0 or clean_doctor_earning = 0 then true
    else false
  end as is_suspicious_revenue_data,

  case
    when status = 0 then 'INVALID'
    when status = 1 then 'CREATED'
    when status = 2 then 'CONFIRMED'
    when status = 3 then 'CANCELLED'
    when status = 4 then 'IN_PROGRESS'
    when status = 5 then 'FINISHED'
    else 'UNKNOWN'
  end as booking_status_name,

  safe.parse_date('%Y-%m-%d', regexp_extract(cast(created_at as string), r'(\d{4}-\d{2}-\d{2})')) as booking_date,
  current_timestamp() as _transformed_at

from joined
