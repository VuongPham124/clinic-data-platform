# DBT Data Model Documentation

## 1) Muc tieu
Tai lieu nay mo ta chi tiet cac model trong `dbt/` de Data Engineer va Data Analyst co the:
- Hieu dung business meaning cua tung model.
- Biet grain (do chi tiet), key, lineage, va rule lam sach.
- Danh gia nhanh chat luong va rui ro van hanh.

Phan vi: `models/platinum` va `models/gold` (bo qua `models/example`).

## 2) Kien truc tong the
- `silver_curated` (source): du lieu nghiep vu raw-curated.
- `platinum` (dbt models): dim/fact da chuan hoa, co tach `valid`/`invalid` cho nhom fact quan trong.
- `gold` (dbt marts): KPI/aggregate phuc vu dashboard va phan tich.

Nguon duoc khai bao tai:
- `models/sources/src_silver.yml`
- `models/sources/src_platinum.yml`

## 3) Quy uoc chung trong project
- Khoa surrogate dung `abs(farm_fingerprint(...))` cho `*_key`.
- Date key dung dinh dang `INT64 yyyymmdd`.
- Nhieu fact lon dung `incremental + merge`.
- Pattern chat luong du lieu:
  - `fact_xxx`: ban goc da transform.
  - `fact_xxx_valid`: tap hop le de downstream su dung.
  - `fact_xxx_invalid`: tap loi + cot `invalid_reason`.

## 3.1) Incremental Merge: cach hoat dong trong project nay

### Co che chung
- `materialized='incremental'` + `incremental_strategy='merge'`.
- Lan dau: dbt tao bang day du (full load cho model do).
- Cac lan sau: dbt chi lay tap du lieu moi/gan day (theo dieu kien `is_incremental()` trong SQL), sau do `MERGE` vao bang dich.
- `unique_key` quyet dinh logic upsert:
  - Trung key: `UPDATE`.
  - Chua co key: `INSERT`.

### Cua so tai xu ly (reprocessing window)
- Phan lon model dang reprocess cua so 31 ngay (`max(key) - 31`) de bat late-arriving records.
- Cac model aggregate theo thang/tuan thuong reprocess 1 chu ky truoc:
  - Theo thang: `max(month_start_date_key) - 100` (lui 1 thang key dang `yyyymmdd`).
  - Theo tuan: `max(week_start_date_key) - 100` (lui 1 tuan theo moc date key).

### Partition va cluster (anh huong hieu nang merge)
- Nhieu model incremental da co `partition_by` + `cluster_by` de:
  - Han che scan khi merge.
  - Tang toc do truy van theo key nghiep vu (`clinic_key`, `doctor_key`, `medicine_key`...).
- Luu y: dang dung partition range tren `INT64 date_key`; can theo doi gioi han partition cua BigQuery.

### Trade-off van hanh
- Uu diem:
  - Giam chi phi va thoi gian so voi full rebuild.
  - Van sua duoc du lieu tre den trong cua so reprocessing.
- Han che:
  - Neu du lieu den tre hon cua so (VD >31 ngay) se can full-refresh hoac backfill co chu dich.
  - Neu `unique_key` khong on dinh co the gay duplicate/overwrite sai.

### Danh sach model dang dung incremental merge
- Platinum:
  - `fact_operational_clinic_bookings`
  - `fact_operational_clinic_bookings_valid`
  - `fact_prescription`
  - `fact_prescription_valid`
  - `fact_inventory_import`
  - `fact_inventory_import_valid`
  - `fact_inventory_export`
  - `fact_inventory_export_valid`
- Gold:
  - `gold_clinic_doctor_operational_efficiency_weekly`
  - `gold_clinic_visits_doctor_monthly`
  - `gold_prescription_value_daily`
  - `gold_disease_top_by_clinic_age_weekly`
  - `gold_best_selling_items`
  - `gold_visits_doctor_monthly`
  - `rev_gold_revenue`

## 4) Platinum Models

### 4.1 Dimensions (Core)

#### `dim_clinics`
- Grain: 1 dong / clinic (`clinic_id`).
- Source: `silver.clinics`.
- Key chinh: `clinic_key`, `clinic_id`.
- Cot nghiep vu: `clinic_name`, `clinic_address`, `is_active`, `admin_user_id`, `open_time`, `close_time`.
- Lam sach: dedup theo `clinic_id`.

#### `dim_clinic_doctors`
- Grain: 1 dong / doctor (`doctor_id`).
- Source: `silver.clinic_doctors`.
- Key: `doctor_key`, `doctor_id`.
- Cot nghiep vu: `doctor_name`, `clinic_doctor_id`.
- Lam sach: dedup theo `doctor_id`.

#### `dim_clinic_patients`
- Grain: 1 dong / patient (`patient_id`).
- Source: `silver.clinic_patients`.
- Key: `patient_key`, `patient_id`.
- Cot nghiep vu: `patient_user_id`, `clinic_patient_id`.
- Lam sach: group theo `patient_id`, lay `any_value` cho cac cot con lai.

#### `dim_clinic_rooms`
- Grain: 1 dong / room (`room_id`).
- Source: `silver.clinic_rooms`.
- Key lien ket: `clinic_key`, `doctor_key`, `room_id`.
- Cot nghiep vu: `room_name`.
- Lam sach: dedup theo `room_id`.

#### `dim_date`
- Grain: 1 dong / ngay.
- Sinh du lieu bang `generate_date_array(2015-01-01 -> 2035-12-31)`.
- Key: `date_key`.
- Cot quan trong: `full_date`, `year`, `month`, `week_of_year`, `is_weekend`, `month_start_date_key`.

#### `dim_medicines`
- Grain: 1 dong / medicine (`medicine_id`).
- Source: `silver.medicines`.
- Key: `medicine_key`, `medicine_id`.
- Cot nghiep vu: `medicine_name`, `code`, `medicine_type`, `medicine_group`, `unit`, `manufacturer`, `vat`.

#### `dim_medicines_lot`
- Grain: 1 dong / lo nhap (`medicine_import_detail_id`).
- Source: `silver.medicine_import_details`.
- Key: `lot_key`, `medicine_import_detail_id`.
- Cot nghiep vu: `lot_number`, `manufacturing_date`, `expire_date`, `import_price`, `status`.
- Lam sach: parse date theo nhieu format + `distinct`.

### 4.2 Dimensions/Facts (Legacy Revenue)

#### `rev_dim_doctors`
- Grain: 1 dong / doctor (`doctor_id`).
- Source: `silver.doctors` + `silver.public_users`.
- Key: `doctor_key`, `doctor_id`.
- Cot bo sung: `user_full_name`.

#### `rev_dim_patients`
- Grain: 1 dong / patient (`patient_id`).
- Source: `silver.patients`.
- Key: `patient_key`, `patient_id`.

#### `rev_dim_prescription_medicines`
- Grain: 1 dong / prescription_medicine.
- Source: `silver.prescription_medicines`.
- Muc tieu: clean numeric (`clean_list_price`, `clean_unit_price`, `clean_vat_amount`) va parse `booking_date`.

#### `rev_fact_bookings`
- Grain: 1 dong / booking online (`silver.bookings.id`).
- Key: `id` (goc), bo sung `doctor_key`, `patient_key`.
- Metric chinh: `amaz_net_revenue`, `is_valid_online_revenue`, `is_suspicious_revenue_data`, `booking_status_name`.

### 4.3 Facts (Core)

#### `fact_operational_clinic_bookings`
- Grain: 1 dong / booking (`booking_id`).
- Source: `silver.clinic_bookings`.
- Key lien ket: `clinic_key`, `doctor_key`, `patient_key`, `room_id`.
- Time keys: `created_date_key`, `from_date_key`, `confirmed_date_key`, `finished_date_key`, `canceled_date_key`.
- Metric/logic:
  - `confirm_duration_sec`, `consult_duration_sec`.
  - Revenue fields: `service_amount`, `prescription_amount`, `commission_fee`, `voucher_amount`, `total_bill`, `amaz_revenue`, `clinic_net_revenue`.
  - Payment mapping: `payment_channel`, `paid_via_app`, `clinic_cash_received`.
  - `is_revenue_eligible` theo `exam_status='finished'` va `payment_status='paid'`.
- Materialization: incremental merge.

#### `fact_operational_clinic_bookings_valid`
- Loc ban ghi du dieu kien downstream:
  - key bat buoc non-null.
  - duration bat buoc khi `is_completed=true`.
- Materialization: incremental merge.

#### `fact_operational_clinic_bookings_invalid`
- Chua ban ghi loi + `invalid_reason`.
- Ly do loi chinh: null key/date; completed nhung thieu duration.

#### `fact_prescription`
- Grain: 1 dong / prescription (`prescription_id`).
- Source: `silver.prescriptions`.
- Key lien ket: `clinic_key`, `doctor_key`, `room_id`.
- Metric chinh: `price`, `price_included_vat`, `vat_amount`.
- Materialization: incremental merge.

#### `fact_prescription_valid` / `fact_prescription_invalid`
- `valid`: giu ban ghi co day du key/date.
- `invalid`: ban ghi loi + `invalid_reason`.

#### `fact_health_record`
- Grain: 1 dong / health record (`health_record_id`).
- Source: `silver.health_records`.
- Key: `clinic_key`, `record_date_key`.
- Rule bo sung: map `patient_age` -> `age_group_key` (1..5).
- Materialization: view.

#### `fact_health_record_valid` / `fact_health_record_invalid`
- Tach ban ghi hop le va ban ghi loi theo null checks (`health_record_id`, `clinic_key`, `record_date_key`, `age_group_key`).

#### `fact_disease_record`
- Grain: 1 dong / disease record (`disease_record_id`).
- Source: `silver.disease_records`.
- Cot chinh: `health_record_id`, `disease_code`, `disease_name`.
- Materialization: view.

#### `fact_disease_record_valid` / `fact_disease_record_invalid`
- Tach theo null checks (`disease_record_id`, `health_record_id`, `disease_code`).

#### `fact_inventory_import`
- Grain: 1 dong / medicine import detail (`import_fact_id`).
- Source: `silver.medicine_import_details` (`status='active'`).
- Key lien ket: `date_key`, `clinic_key`, `medicine_key`, `lot_key`.
- Metric chinh: `quantity_imported`, `initial_import_value`.
- Materialization: incremental merge.

#### `fact_inventory_import_valid` / `fact_inventory_import_invalid`
- `valid`: non-null toan bo key chinh.
- `invalid`: ban ghi loi + `invalid_reason`.

#### `fact_inventory_export`
- Grain: 1 dong / prescription medicine detail (`export_fact_id`).
- Source: `silver.prescription_medicine_details` + `silver.prescription_medicines`.
- Key lien ket: `date_key`, `clinic_key`, `medicine_key`, `lot_key`.
- Metric: `quantity_exported`.
- Materialization: incremental merge.

#### `fact_inventory_export_valid` / `fact_inventory_export_invalid`
- Tach du lieu hop le/khong hop le theo null checks.

#### `fact_inventory_snapshot`
- Grain: 1 dong / lot ton kho (`snapshot_fact_id` = `medicine_import_detail_id`).
- Nguon logic:
  - Import active tu `silver.medicine_import_details`.
  - Tru di tong export theo lot tu `fact_inventory_export`.
- Metric chinh:
  - `current_quantity = quantity_imported - total_exported`.
  - `current_inventory_value = current_quantity * import_price`.
  - `days_to_expire`.
- Materialization: view.

#### `fact_inventory_snapshot_valid` / `fact_inventory_snapshot_invalid`
- `valid`: non-null key + `current_quantity > 0`.
- `invalid`: null key hoac `current_quantity <= 0`.

## 5) Gold Models (Marts/KPI)

#### `gold_clinic_doctor_operational_efficiency_weekly`
- Grain: 1 dong / clinic / tuan (`week_start_date_key`).
- Input: `fact_operational_clinic_bookings_valid`, `dim_date`, `dim_clinics`.
- KPI: `completed_booking_count`, `avg_confirm_duration_sec`, `avg_consult_duration_sec`, `cancellation_rate`.

#### `gold_clinic_visits_doctor_monthly`
- Grain: 1 dong / clinic / thang.
- KPI: `number_booking`.
- Input: booking valid + dim date + dim clinic.

#### `gold_clinic_patient_visits_monthly`
- Grain: 1 dong / clinic / thang.
- KPI:
  - `new_visits`: patient co lan dau trong thang.
  - `returning_visits`: patient quay lai.
  - `retention_rate`.

#### `gold_prescription_value_daily`
- Grain: 1 dong / clinic / doctor / room / ngay.
- KPI: `prescription_count`, `total_value`, `avg_price_effective`.
- Rule gia: uu tien `price_included_vat`, fallback `price`.

#### `gold_disease_top_by_clinic_age_weekly`
- Grain: 1 dong / clinic / tuan / age_group / disease_code.
- KPI: `total_occurrences`, `unique_records`.
- Input: `fact_health_record_valid` + `fact_disease_record_valid`.

#### `gold_best_selling_items`
- Grain: 1 dong / clinic_name / medicine_name / year / month.
- KPI: `total_quantity_exported`, `rank` theo clinic-thang.

#### `gold_low_stock_items`
- Grain: 1 dong / clinic / medicine.
- KPI: `total_current_quantity`, `items_sold_prev_month`, `is_low_stock`.
- Rule canh bao: ton hien tai < ban thang truoc.

#### `gold_near_expiry_drugs`
- Grain: 1 dong / lot.
- KPI/flag: `days_to_expire`, `urgent_level` (`red/yellow/green`).

#### `gold_slow_moving_items`
- Grain: 1 dong / lot ton kho.
- KPI/flag: `days_in_stock`, `is_slow_moving`.
- Rule: lot co ton > 0 va khong co giao dich xuat trong 3 thang gan nhat.

#### `gold_visits_doctor_monthly`
- Grain: 1 dong / doctor / thang (luong legacy online).
- KPI: `number_booking`.
- Input: `rev_fact_bookings` + `rev_dim_doctors` + `dim_date`.

#### `rev_gold_revenue`
- Grain: 1 dong / booking / source_type (`OFFLINE` + `ONLINE`).
- Muc tieu: hop nhat doanh thu online/offline vao mot mart.
- KPI: `gross_revenue`, `amaz_revenue`, `partner_share`, `service_amount_only`, `medicine_amount_total`.
- Materialization: incremental merge.

## 6) Mapping nhanh cho Data Analyst

### Cau hoi nghiep vu va model de dung
- Hieu qua van hanh phong kham: `gold_clinic_doctor_operational_efficiency_weekly`.
- Luong kham theo clinic-thang: `gold_clinic_visits_doctor_monthly`.
- Benh nhan moi/quay lai: `gold_clinic_patient_visits_monthly`.
- Gia tri ke don theo ngay: `gold_prescription_value_daily`.
- Benh pho bien theo tuoi: `gold_disease_top_by_clinic_age_weekly`.
- Ton kho/rui ro kho: `gold_low_stock_items`, `gold_near_expiry_drugs`, `gold_slow_moving_items`, `gold_best_selling_items`.
- Doanh thu tong hop online/offline: `rev_gold_revenue`.

### Khi can truy vet du lieu goc
- Gold -> Platinum valid -> Platinum base -> Silver source.
- Neu KPI bat thuong, check model `*_invalid` cung domain de tim root-cause.

## 7) Danh gia ky thuat va van hanh (DE view)

### Diem manh
- Da co star-schema ro rang (dim/fact).
- Co tach `valid/invalid` cho cac fact trong yeu.
- Nhieu fact/gold lon da dung incremental merge.

### Rui ro / diem can luu y
- Nhieu model partition theo `INT64 range` voi khoang `20000101 -> 21000101, interval=100`.
- Cau hinh nay co the tao so partition kha lon; can theo doi loi BigQuery "possible partitions exceeding limit".
- `fact_inventory_snapshot` dang la `view` va join `fact_inventory_export`; voi du lieu lon co the ton cost query.
- `gold_clinic_doctor_operational_efficiency_weekly` dang tinh `cancellation_rate` voi dieu kien `canceled_date_key=10101`; can xac nhan day la quy uoc dung.
- Project chua mo ta SLA, owner, freshness expectation cho tung model ngay trong schema docs.

### Goi y uu tien tiep theo
- Chuyen sang partition theo cot `DATE` hoac theo month key de giam rui ro partition limit.
- Bo sung `description` cho model va columns trong `schema.yml` de dbt docs day du hon.
- Dat policy test severity ro rang (`error/warn`) theo impact business.
- Bo sung exposure/dashboard mapping de trace tu BI ve model.

## 8) Lenh chay thuong dung
- Build platinum:
  - `dbt build --target platinum --select path:models/platinum --profiles-dir . --project-dir .`
- Build gold:
  - `dbt build --target gold --select path:models/gold --profiles-dir . --project-dir .`
- Build mot model + downstream:
  - `dbt build --select +gold_clinic_patient_visits_monthly --profiles-dir . --project-dir .`

## 9) File lien quan
- `dbt/dbt_project.yml`
- `dbt/profiles.yml`
- `dbt/models/platinum/schema.yml`
- `dbt/models/gold/schema.yml`
- `dbt/models/sources/src_silver.yml`
- `dbt/models/sources/src_platinum.yml`
