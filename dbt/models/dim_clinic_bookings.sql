WITH base_data AS (
  SELECT 
    cb.*,

    -- CLEAN
    CAST(
      COALESCE(NULLIF(JSON_VALUE(cb.metadata, '$.services.total_amount'), ''), '0')
      AS NUMERIC
    ) AS service_amount,

    CAST(
      COALESCE(NULLIF(CAST(cb.prescription_amount AS STRING), ''), '0')
      AS NUMERIC
    ) AS clean_prescription_amount,

    CAST(
      COALESCE(NULLIF(CAST(cb.clinic_commission_fee AS STRING), ''), '0')
      AS NUMERIC
    ) AS commission_fee,

    CAST(
      COALESCE(NULLIF(CAST(cb.patient_discount_amount AS STRING), ''), '0')
      AS NUMERIC
    ) AS voucher_amount,

    -- Tiền thực thu
    COALESCE(
      (
        SELECT SUM(CAST(JSON_VALUE(item, '$.amount') AS NUMERIC))
        FROM UNNEST(JSON_QUERY_ARRAY(cb.metadata, '$.payment_data')) AS item
      ),
      0
    ) AS paid_amount

  FROM `wata-clinicdataplatform-gcp.silver.public_clinic_bookings` cb
  WHERE cb.deleted_at IS NULL
)

SELECT
  base_data.*,

  -- BILL
  (service_amount + clean_prescription_amount) AS total_bill,

  -- PHÂN LOẠI THANH TOÁN
  CASE 
    WHEN payment_method IN ('advance', 'full_payment') THEN 'OFFLINE_APP'
    WHEN payment_method = 'direct_payment' THEN 'OFFLINE_CASH'
    ELSE 'UNKNOWN'
  END AS payment_channel,

  -- DÒNG TIỀN
  CASE 
    WHEN payment_method = 'direct_payment' THEN paid_amount
    ELSE 0
  END AS clinic_cash_received,

  CASE 
    WHEN payment_method IN ('advance', 'full_payment') THEN paid_amount
    ELSE 0
  END AS paid_via_app,

  -- AMAZ REVENUE
  CASE 
    WHEN payment_method IN ('advance', 'full_payment', 'direct_payment')
    THEN (commission_fee - voucher_amount)
    ELSE 0
  END AS amaz_revenue,

  -- CLINIC NET REVENUE (chỉ cash)
  CASE 
    WHEN payment_method = 'direct_payment'
    THEN (service_amount + clean_prescription_amount - commission_fee)
    ELSE NULL
  END AS clinic_net_revenue,

  -- STATUS
  CASE 
    WHEN exam_status = 'finished' AND payment_status = 'paid' THEN 'COMPLETED'
    WHEN exam_status = 'cancelled' THEN 'CANCELLED'
    WHEN exam_status IN ('waiting','data_collection','in_progress') THEN 'IN_PROGRESS'
    ELSE 'OTHER'
  END AS booking_status_group,

  CASE 
  WHEN exam_status = 'finished'
    AND payment_status = 'paid'
    THEN TRUE
    ELSE FALSE
  END AS is_revenue_eligible,
  
  -- Làm sạch chuỗi timestamp để sửa lỗi date dimension
  SAFE.PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(CAST(created_at AS STRING), r'(\d{4}-\d{2}-\d{2})')) AS booking_date,

  CURRENT_TIMESTAMP() AS _transformed_at

FROM base_data