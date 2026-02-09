SELECT 
    CAST(id AS STRING) AS booking_id,
    'OFFLINE' AS source_type,
    clinic_id,
    doctor_id,
    patient_id,
    booking_date,
    total_bill AS gross_revenue,
    amaz_revenue,
    CASE 
      WHEN payment_method = 'direct_payment' THEN (total_bill - commission_fee)
      WHEN payment_method IN ('advance', 'full_payment') THEN paid_via_app
      ELSE NULL
    END AS partner_share,
    service_amount AS service_amount_only,
    clean_prescription_amount AS medicine_amount_total,
    payment_channel,
    is_revenue_eligible AS is_valid_for_revenue
FROM {{ ref('dim_clinic_bookings') }}
WHERE is_revenue_eligible = TRUE 

UNION ALL

SELECT 
    CAST(id AS STRING) AS booking_id,
    'ONLINE' AS source_type,
    NULL AS clinic_id,
    doctor_id,
    patient_id,
    booking_date,
    clean_price AS gross_revenue,
    amaz_net_revenue AS amaz_revenue,
    clean_doctor_earning AS partner_share,
    clean_price AS service_amount_only,
    0 AS medicine_amount_total,
    'ONLINE_APP' AS payment_channel,
    is_valid_online_revenue AS is_valid_for_revenue
FROM {{ ref('dim_bookings') }}
WHERE is_valid_online_revenue = TRUE