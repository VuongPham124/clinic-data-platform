-- ============================================================
-- Tạo bảng __pipeline_config
-- ============================================================
CREATE TABLE IF NOT EXISTS `wata-clinicdataplatform-gcp.manual.__pipeline_config` (
  file_key        STRING  NOT NULL,
  step_order      INT64   NOT NULL,
  target_table    STRING,
  query_template  STRING  NOT NULL,
  is_active       BOOL    NOT NULL
);


-- ============================================================
-- File 1: medicine_categories
-- ============================================================
INSERT INTO `wata-clinicdataplatform-gcp.manual.__pipeline_config`
  (file_key, step_order, target_table, query_template, is_active)
VALUES (
  'medicine_categories',
  1,
  'public_medicines',
  """MERGE `{project}.{silver_dataset}.public_medicines` T
USING (
  SELECT S.*,
    ABS(FARM_FINGERPRINT(CONCAT(GENERATE_UUID(), S.ma_thuoc))) AS new_id
  FROM `{project}.{dataset}.medicine_categories` S
) S
ON T.code = S.ma_thuoc
WHEN MATCHED THEN
  UPDATE SET
    name = S.ten_thuoc,
    dqg_code = S.ma_duoc_qg,
    type = S.loai_hang_hoa,
    `group` = S.nhom_thuoc,
    packing = S.quy_cach_dong_goi,
    unit = S.don_vi_tinh,
    usage = S.duong_dung,
    active_element = S.hoat_chat,
    concentration = S.nong_do_ham_luong,
    price = S.gia_ban,
    manufacturer = S.hang_san_xuat,
    country_of_origin = S.nuoc_san_xuat,
    vat = S.vat,
    is_deleted = FALSE,
    __lsn_num = 2000,
    __commit_ts = CURRENT_TIMESTAMP(),
    updated_at = CAST(CURRENT_TIMESTAMP() AS STRING)
WHEN NOT MATCHED THEN
  INSERT (
    id, name, code, dqg_code, type, `group`,
    packing, unit, usage, active_element,
    concentration, price, manufacturer,
    country_of_origin, vat,
    is_deleted, __lsn_num, __commit_ts,
    created_at, updated_at
  )
  VALUES (
    S.new_id,
    S.ten_thuoc, S.ma_thuoc, S.ma_duoc_qg, S.loai_hang_hoa, S.nhom_thuoc,
    S.quy_cach_dong_goi, S.don_vi_tinh, S.duong_dung, S.hoat_chat,
    S.nong_do_ham_luong, S.gia_ban, S.hang_san_xuat, S.nuoc_san_xuat,
    S.vat,
    FALSE, 2000, CURRENT_TIMESTAMP(),
    CAST(CURRENT_TIMESTAMP() AS STRING),
    CAST(CURRENT_TIMESTAMP() AS STRING)
  )""",
  TRUE
);


-- ============================================================
-- File 2: inventory
-- Step 1: tạo import header
-- ============================================================
INSERT INTO `wata-clinicdataplatform-gcp.manual.__pipeline_config`
  (file_key, step_order, target_table, query_template, is_active)
VALUES (
  'inventory',
  1,
  'public_medicine_imports',
  """INSERT INTO `{project}.{silver_dataset}.public_medicine_imports`
  (id, code, import_date, is_deleted, __lsn_num, __commit_ts, created_at, updated_at)
VALUES (
  ABS(FARM_FINGERPRINT(GENERATE_UUID())),
  CONCAT('IMP_', FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP())),
  CAST(CURRENT_DATE() AS STRING),
  FALSE,
  2000,
  CURRENT_TIMESTAMP(),
  CAST(CURRENT_TIMESTAMP() AS STRING),
  CAST(CURRENT_TIMESTAMP() AS STRING)
)""",
  TRUE
);

-- Step 2: tạo import details
INSERT INTO `wata-clinicdataplatform-gcp.manual.__pipeline_config`
  (file_key, step_order, target_table, query_template, is_active)
VALUES (
  'inventory',
  2,
  'public_medicine_import_details',
  """INSERT INTO `{project}.{silver_dataset}.public_medicine_import_details`
  (id, medicine_import_id, medicine_id, total, expire_date, import_price,
   is_deleted, __lsn_num, __commit_ts, created_at, updated_at)
SELECT
  ABS(FARM_FINGERPRINT(CONCAT(GENERATE_UUID(), CAST(m.id AS STRING)))),
  i.import_id,
  m.id,
  SAFE_CAST(s.so_luong AS INT64),
  s.ngay_het_han,
  s.gia_nhap,
  FALSE,
  2000,
  CURRENT_TIMESTAMP(),
  CAST(CURRENT_TIMESTAMP() AS STRING),
  CAST(CURRENT_TIMESTAMP() AS STRING)
FROM `{project}.{dataset}.inventory` s
JOIN `{project}.{silver_dataset}.public_medicines` m
  ON s.ma_thuoc = m.code
CROSS JOIN (
  SELECT id AS import_id
  FROM `{project}.{silver_dataset}.public_medicine_imports`
  ORDER BY created_at DESC
  LIMIT 1
) i""",
  TRUE
);


-- ============================================================
-- File 3: patients
-- Step 1: insert public_users
-- ============================================================
INSERT INTO `wata-clinicdataplatform-gcp.manual.__pipeline_config`
  (file_key, step_order, target_table, query_template, is_active)
VALUES (
  'patients',
  1,
  'public_users',
  """INSERT INTO `{project}.{silver_dataset}.public_users`
  (id, full_name, phone_number, email, gender,
   date_of_birth, date, month, year, address, role,
   is_deleted, __lsn_num, __commit_ts,
   created_at, updated_at)
SELECT
  ABS(FARM_FINGERPRINT(CONCAT(GENERATE_UUID(), S.phone_number))),
  S.name,
  S.phone_number,
  S.email,
  CASE
    WHEN LOWER(S.gender) = 'male'   THEN 1
    WHEN LOWER(S.gender) = 'female' THEN 2
    ELSE 3
  END,
  CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', S.birthday) AS STRING),
  EXTRACT(DAY   FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', S.birthday)),
  EXTRACT(MONTH FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', S.birthday)),
  EXTRACT(YEAR  FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', S.birthday)),
  S.address,
  1,
  FALSE, 2000, CURRENT_TIMESTAMP(),
  CAST(CURRENT_TIMESTAMP() AS STRING),
  CAST(CURRENT_TIMESTAMP() AS STRING)
FROM `{project}.{dataset}.patients` S""",
  TRUE
);

-- Step 2: insert public_patients
INSERT INTO `wata-clinicdataplatform-gcp.manual.__pipeline_config`
  (file_key, step_order, target_table, query_template, is_active)
VALUES (
  'patients',
  2,
  'public_patients',
  """INSERT INTO `{project}.{silver_dataset}.public_patients`
  (id, user_id, is_deleted, __lsn_num, __commit_ts, created_at, updated_at)
SELECT
  ABS(FARM_FINGERPRINT(CONCAT(GENERATE_UUID(), CAST(u_id AS STRING)))),
  u_id,
  FALSE,
  2000,
  CURRENT_TIMESTAMP(),
  CAST(CURRENT_TIMESTAMP() AS STRING),
  CAST(CURRENT_TIMESTAMP() AS STRING)
FROM (
  SELECT u.id AS u_id
  FROM `{project}.{silver_dataset}.public_users` u
  JOIN `{project}.{dataset}.patients` s
    ON u.phone_number = s.phone_number
  WHERE u.id NOT IN (
    SELECT user_id FROM `{project}.{silver_dataset}.public_patients`
  )
  QUALIFY ROW_NUMBER() OVER(PARTITION BY u.phone_number ORDER BY u.id DESC) = 1
) sub""",
  TRUE
);