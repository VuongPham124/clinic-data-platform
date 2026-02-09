SELECT 
    pm.id AS item_id,
    pm.prescription_id,
    pm.clinic_id,
    pm.medicine_id,
    pm.booking_date AS sale_date,
    CAST(pm.total AS NUMERIC) AS quantity,
    pm.clean_unit_price AS unit_price,
    pm.clean_list_price AS total_price,
    pm.clean_vat_amount AS vat_amount,
    (pm.clean_list_price - pm.clean_vat_amount) AS total_price_net_of_vat
FROM {{ ref('dim_prescription_medicines') }} pm