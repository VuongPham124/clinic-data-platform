# DBT README

## Purpose
Folder `dbt/` chứa project dbt để xây mô hình dữ liệu tầng `platinum` (dim/fact) và `gold` (mart/KPI phục vụ BI).

## Project Files

### `dbt_project.yml`
- Tên project: `clinic_platinum`.
- Profile dùng: `clinic_platinum`.
- Materialization m?c d?nh:
  - `models/platinum`: `table`
  - `models/gold`: `table`
- B?t models trong `sources`.

### `profiles.yml`
- Ðịnh nghĩa 2 outputs BigQuery:
  - `platinum` -> dataset `platinum`
  - `gold` -> dataset `gold`
- Cấu hình qua env vars (`DBT_GCP_PROJECT`, `DBT_BQ_LOCATION`, `DBT_THREADS`, ...).
- `target` runtime lấy từ `DBT_TARGET` (mặc định `platinum`).

## Sources

### `models/sources/src_silver.yml`
- Source `silver` đọc từ dataset `silver_curated`.
- Khai báo các bảng operational/prescription/health/revenue cần cho platinum:
  - `clinic_bookings`, `clinics`, `clinic_doctors`, `clinic_patients`, `clinic_rooms`
  - `prescriptions`, `prescription_medicines`, `prescription_medicine_details`
  - `health_records`, `disease_records` (`public_icd10_diagnoses`)
  - `medicines`, `medicine_import_details`
  - `bookings`, `doctors`, `patients`

### `models/sources/src_platinum.yml`
- Source `platinum` cho tầng gold, gồm các dim/fact valid và bảng revenue:
  - dims: `dim_date`, `dim_clinic_*`, `dim_medicines`, `dim_medicines_lot`
  - facts valid: inventory/import/export/snapshot, booking operational, prescription, health, disease
  - `rev_fact_bookings`

## Schema/Test Files

### `models/platinum/schema.yml`
- Test khóa chính cho dims/facts (not_null/unique cho các key cốt lõi).

### `models/gold/schema.yml`
- Test chuẩn định danh và thời gian/chỉ số cho các marts gold.

### `models/example/schema.yml`
- File mẫu dbt starter (my_first/my_second models).

## Platinum Models

### Dims (`models/platinum/dims`)
- `dim_clinics.sql`: dimension clinic (id, name, address, active, admin_user_id, open/close time).
- `dim_clinic_doctors.sql`: dimension doctor theo clinic domain.
- `dim_clinic_patients.sql`: dimension patient theo clinic domain.
- `dim_clinic_rooms.sql`: dimension room trong clinic.
- `dim_date.sql`: date dimension từ 2015-01-01 đến 2035-12-31, có `month_start_date_key`.
- `dim_medicines.sql`: dimension medicines (name/code/type/group/unit/manufacturer/vat).
- `dim_medicines_lot.sql`: dimension lot (lot number, MFG/expiry/import price/status).
- `rev_dim_doctors.sql`: view doctor dimension cho luồng revenue legacy.
- `rev_dim_patients.sql`: view patient dimension cho luồng revenue legacy.
- `rev_dim_prescription_medicines.sql`: làm sạch giá/vat và parse booking date cho prescription medicines.

### Facts Core (`models/platinum/facts`)
- `fact_operational_clinic_bookings.sql`: fact vận hành booking (timestamps, trạng thái, durations, revenue fields).
- `fact_operational_clinic_bookings_valid.sql`: tập valid records booking (view).
- `fact_operational_clinic_bookings_invalid.sql`: tập invalid records booking + `invalid_reason`.

- `fact_prescription.sql`: fact kê don (clinic/doctor/date/value VAT).
- `fact_prescription_valid.sql`: filter bản ghi kê don hợp lệ.
- `fact_prescription_invalid.sql`: bản ghi kê đơn ko hợp lệ + reason.

- `fact_health_record.sql`: fact hồ so khám bệnh (clinic_key, date_key, age_group_key).
- `fact_health_record_valid.sql`: tập health record hợp lệ.
- `fact_health_record_invalid.sql`: tập health record k hợp lệ + reason.

- `fact_disease_record.sql`: fact bệnh chẩn đoán health_record.
- `fact_disease_record_valid.sql`:  disease record hợp lệ.
- `fact_disease_record_invalid.sql`: disease record k hợp lệ + reason.

- `fact_inventory_import.sql`: fact nhập kho từ medicine import details.
- `fact_inventory_import_valid.sql`: bản ghi nhập kho hợp lí.
- `fact_inventory_import_invalid.sql`: bản ghi nhập kho lỗi + reason.

- `fact_inventory_export.sql`: fact xuất kho từ prescription_medicine_details.
- `fact_inventory_export_valid.sql`: bản ghi xuất kho hợp lí.
- `fact_inventory_export_invalid.sql`: bản ghi xuất kho lỗi + reason.

- `fact_inventory_snapshot.sql`: kho snapshot = nhập - xuất theo lot/clinic/medicine.
- `fact_inventory_snapshot_valid.sql`: bản ghi ok (`current_quantity > 0`).
- `fact_inventory_snapshot_invalid.sql`: bản ghi lỗi + reason.

### Revenue Legacy
- `rev_fact_bookings.sql`: fact doanh thu legacy từ `silver.bookings`, làm sạch các numeric amounts và map keys.

## Gold Models (`models/gold`)
- `gold_clinic_doctor_operational_efficiency_weekly.sql`: KPI hiệu quả vận hành bác si theo tu?n.
- `gold_clinic_visits_doctor_monthly.sql`: lượt khám theo clinic/doctor theo tháng.
- `gold_clinic_patient_visits_monthly.sql`: số lượt bệnh nhân quay l?i theo tháng.
- `gold_disease_top_by_clinic_age_weekly.sql`: top bệnh theo clinic + nhóm tuổi theo tuần.
- `gold_prescription_value_daily.sql`: tổng trị giá kê don theo ngày/clinic/doctor/room.
- `gold_best_selling_items.sql`: thuốc bán chạy theo clinic-tháng (ranking).
- `gold_low_stock_items.sql`: cảnh báo kho thuốc dựa snapshot + lịch sử xuất.
- `gold_near_expiry_drugs.sql`: cảnh báo thuốcc gần hết hạn.
- `rev_gold_revenue.sql`: bản revenue gold (hiện dang comment/legacy draft).

## Run Commands (local)
- `dbt deps`
- `dbt debug --profiles-dir . --project-dir .`
- `dbt build --target platinum --select path:models/platinum --profiles-dir . --project-dir .`
- `dbt build --target gold --select path:models/gold --profiles-dir . --project-dir .`

## Notes
- Các model `*_valid` / `*_invalid` giúp tách dữ liệu sạch và lỗi giám sát chất lượng.
- `rev_*` là nhánh legacy/revenue, tồn tại song song starchema chính

