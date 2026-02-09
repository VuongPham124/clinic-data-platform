WITH base_data AS (
  SELECT 
    b.*,

    -- CLEAN & CAST TIỀN (Sửa ::text thành CAST AS STRING)
    CAST(
      COALESCE(NULLIF(CAST(b.patient_paying_amount AS STRING), ''), '0') 
      AS NUMERIC
    ) AS clean_patient_paying,

    CAST(
      COALESCE(NULLIF(CAST(b.doctor_earning_amount AS STRING), ''), '0') 
      AS NUMERIC
    ) AS clean_doctor_earning,

    CAST(
      COALESCE(NULLIF(CAST(b.patient_discount_amount AS STRING), ''), '0') 
      AS NUMERIC
    ) AS clean_patient_discount,

    CAST(
      COALESCE(NULLIF(CAST(b.price AS STRING), ''), '0') 
      AS NUMERIC
    ) AS clean_price,

    CAST(
      COALESCE(NULLIF(CAST(b.service_vat AS STRING), ''), '0') 
      AS NUMERIC
    ) AS clean_service_vat

  FROM `wata-clinicdataplatform-gcp.silver.public_bookings` b
  WHERE b.deleted_at IS NULL
    -- Sửa toán tử ~ thành REGEXP_CONTAINS
    AND REGEXP_CONTAINS(CAST(b.price AS STRING), r'^[0-9.]+$')
)
SELECT
  -- CỘT GỐC 
  base_data.*,

  -- AMAZ NET REVENUE
  (clean_patient_paying - clean_doctor_earning) AS amaz_net_revenue,

  -- FLAG NGHIỆP VỤ PHỤC VỤ FACT
  CASE 
    WHEN status = 5 
     AND clean_price > 0
    THEN TRUE
    ELSE FALSE
  END AS is_valid_online_revenue,

  -- DATA QUALITY FLAG
  CASE 
    WHEN clean_patient_paying = 0 
      OR clean_doctor_earning = 0
    THEN TRUE
    ELSE FALSE
  END AS is_suspicious_revenue_data,

  -- STATUS MAP
  CASE 
    WHEN status = 0 THEN 'INVALID'
    WHEN status = 1 THEN 'CREATED'
    WHEN status = 2 THEN 'CONFIRMED'
    WHEN status = 3 THEN 'CANCELLED'
    WHEN status = 4 THEN 'IN_PROGRESS'
    WHEN status = 5 THEN 'FINISHED'
    ELSE 'UNKNOWN'
  END AS booking_status_name,

  -- CỘT LIÊN KẾT VỚI BẢNG DIM_DATE
  SAFE.PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(CAST(created_at AS STRING), r'(\d{4}-\d{2}-\d{2})')) AS booking_date,

  CURRENT_TIMESTAMP() AS _transformed_at

FROM base_data