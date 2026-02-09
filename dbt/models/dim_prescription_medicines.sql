SELECT 
    *,
    -- Nếu là rác (chứa chữ "sáng", "usage_time") thì ép về 0
    COALESCE(SAFE_CAST(price AS NUMERIC), 0) as clean_list_price,
    COALESCE(SAFE_CAST(unit_price AS NUMERIC), 0) as clean_unit_price,
    COALESCE(SAFE_CAST(vat_amount AS NUMERIC), 0) as clean_vat_amount,

    -- Lọc bỏ dấu ngoặc nhọn { } và số 0 thừa để lấy đúng ngày
    SAFE.PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(CAST(created_at AS STRING), r'(\d{4}-\d{2}-\d{2})')) AS booking_date
    
FROM `wata-clinicdataplatform-gcp.silver.public_prescription_medicines`
WHERE deleted_at IS NULL