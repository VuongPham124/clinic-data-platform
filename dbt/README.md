# DBT Data Model Documentation

## 1) Objective
This document describes in detail the models in `dbt/` so that Data Engineers and Data Analysts can:
- Understand the business meaning of each model.
- Know the grain, keys, lineage, and cleaning rules.
- Quickly assess data quality and operational risks.

Scope: `models/platinum` and `models/gold` (excluding `models/example`).

## 2) Overall Architecture
- `silver_curated` (source): raw-curated business data.
- `platinum` (dbt models): normalized dims/facts with `valid`/`invalid` split for key fact groups.
- `gold` (dbt marts): KPIs/aggregates serving dashboards and analysis.

Sources are declared in:
- `models/sources/src_silver.yml`
- `models/sources/src_platinum.yml`

## 3) Project-wide Conventions
- Surrogate keys use `abs(farm_fingerprint(...))` for `*_key`.
- Date keys use `INT64 yyyymmdd` format.
- Large facts use `incremental + merge`.
- Data quality pattern:
  - `fact_xxx`: base transformed table.
  - `fact_xxx_valid`: clean subset for downstream use.
  - `fact_xxx_invalid`: error rows + `invalid_reason` column.

## 3.1) Incremental Merge: How It Works in This Project

### General Mechanism
- `materialized='incremental'` + `incremental_strategy='merge'`.
- First run: dbt creates the full table (full load for that model).
- Subsequent runs: dbt only processes new/recent data (via `is_incremental()` condition in SQL), then `MERGE` into the target table.
- `unique_key` determines upsert logic:
  - Matching key: `UPDATE`.
  - New key: `INSERT`.

### Reprocessing Window
- Most models reprocess a 31-day window (`max(key) - 31`) to capture late-arriving records.
- Monthly/weekly aggregate models typically reprocess 1 prior cycle:
  - Monthly: `max(month_start_date_key) - 100` (steps back 1 month in `yyyymmdd` key format).
  - Weekly: `max(week_start_date_key) - 100` (steps back 1 week by date key).

### Partitioning and Clustering (Performance Impact on Merge)
- Many incremental models include `partition_by` + `cluster_by` to:
  - Limit scan during merge.
  - Speed up queries by business keys (`clinic_key`, `doctor_key`, `medicine_key`, etc.).
- Note: currently using range partition on `INT64 date_key`; monitor for BigQuery partition count limits.

### Operational Trade-offs
- Advantages:
  - Lower cost and faster runtime compared to full rebuilds.
  - Late-arriving data within the reprocessing window is still corrected.
- Limitations:
  - Data arriving later than the window (e.g., >31 days) requires a full-refresh or intentional backfill.
  - An unstable `unique_key` can cause incorrect duplicates or overwrites.

### Models Currently Using Incremental Merge
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
- Grain: 1 row per clinic (`clinic_id`).
- Source: `silver.clinics`.
- Keys: `clinic_key`, `clinic_id`.
- Business columns: `clinic_name`, `clinic_address`, `is_active`, `admin_user_id`, `open_time`, `close_time`.
- Cleaning: dedup by `clinic_id`.

#### `dim_clinic_doctors`
- Grain: 1 row per doctor (`doctor_id`).
- Source: `silver.clinic_doctors`.
- Keys: `doctor_key`, `doctor_id`.
- Business columns: `doctor_name`, `clinic_doctor_id`.
- Cleaning: dedup by `doctor_id`.

#### `dim_clinic_patients`
- Grain: 1 row per patient (`patient_id`).
- Source: `silver.clinic_patients`.
- Keys: `patient_key`, `patient_id`.
- Business columns: `patient_user_id`, `clinic_patient_id`.
- Cleaning: group by `patient_id`, take `any_value` for remaining columns.

#### `dim_clinic_rooms`
- Grain: 1 row per room (`room_id`).
- Source: `silver.clinic_rooms`.
- Join keys: `clinic_key`, `doctor_key`, `room_id`.
- Business columns: `room_name`.
- Cleaning: dedup by `room_id`.

#### `dim_date`
- Grain: 1 row per day.
- Generated using `generate_date_array(2015-01-01 -> 2035-12-31)`.
- Key: `date_key`.
- Key columns: `full_date`, `year`, `month`, `week_of_year`, `is_weekend`, `month_start_date_key`.

#### `dim_medicines`
- Grain: 1 row per medicine (`medicine_id`).
- Source: `silver.medicines`.
- Keys: `medicine_key`, `medicine_id`.
- Business columns: `medicine_name`, `code`, `medicine_type`, `medicine_group`, `unit`, `manufacturer`, `vat`.

#### `dim_medicines_lot`
- Grain: 1 row per import lot (`medicine_import_detail_id`).
- Source: `silver.medicine_import_details`.
- Keys: `lot_key`, `medicine_import_detail_id`.
- Business columns: `lot_number`, `manufacturing_date`, `expire_date`, `import_price`, `status`.
- Cleaning: multi-format date parsing + `distinct`.

### 4.2 Dimensions/Facts (Legacy Revenue)

#### `rev_dim_doctors`
- Grain: 1 row per doctor (`doctor_id`).
- Source: `silver.doctors` + `silver.public_users`.
- Keys: `doctor_key`, `doctor_id`.
- Additional column: `user_full_name`.

#### `rev_dim_patients`
- Grain: 1 row per patient (`patient_id`).
- Source: `silver.patients`.
- Keys: `patient_key`, `patient_id`.

#### `rev_dim_prescription_medicines`
- Grain: 1 row per prescription medicine.
- Source: `silver.prescription_medicines`.
- Purpose: clean numeric fields (`clean_list_price`, `clean_unit_price`, `clean_vat_amount`) and parse `booking_date`.

#### `rev_fact_bookings`
- Grain: 1 row per online booking (`silver.bookings.id`).
- Key: `id` (source), with added `doctor_key`, `patient_key`.
- Key metrics: `amaz_net_revenue`, `is_valid_online_revenue`, `is_suspicious_revenue_data`, `booking_status_name`.

### 4.3 Facts (Core)

#### `fact_operational_clinic_bookings`
- Grain: 1 row per booking (`booking_id`).
- Source: `silver.clinic_bookings`.
- Join keys: `clinic_key`, `doctor_key`, `patient_key`, `room_id`.
- Time keys: `created_date_key`, `from_date_key`, `confirmed_date_key`, `finished_date_key`, `canceled_date_key`.
- Metrics/logic:
  - `confirm_duration_sec`, `consult_duration_sec`.
  - Revenue fields: `service_amount`, `prescription_amount`, `commission_fee`, `voucher_amount`, `total_bill`, `amaz_revenue`, `clinic_net_revenue`.
  - Payment mapping: `payment_channel`, `paid_via_app`, `clinic_cash_received`.
  - `is_revenue_eligible` based on `exam_status='finished'` and `payment_status='paid'`.
- Materialization: incremental merge.

#### `fact_operational_clinic_bookings_valid`
- Filters rows meeting downstream requirements:
  - All required keys non-null.
  - Duration required when `is_completed=true`.
- Materialization: incremental merge.

#### `fact_operational_clinic_bookings_invalid`
- Contains error rows + `invalid_reason`.
- Main error reasons: null key/date; completed booking with missing duration.

#### `fact_prescription`
- Grain: 1 row per prescription (`prescription_id`).
- Source: `silver.prescriptions`.
- Join keys: `clinic_key`, `doctor_key`, `room_id`.
- Key metrics: `price`, `price_included_vat`, `vat_amount`.
- Materialization: incremental merge.

#### `fact_prescription_valid` / `fact_prescription_invalid`
- `valid`: retains rows with complete keys/dates.
- `invalid`: error rows + `invalid_reason`.

#### `fact_health_record`
- Grain: 1 row per health record (`health_record_id`).
- Source: `silver.health_records`.
- Keys: `clinic_key`, `record_date_key`.
- Additional rule: maps `patient_age` -> `age_group_key` (1..5).
- Materialization: view.

#### `fact_health_record_valid` / `fact_health_record_invalid`
- Splits valid and error rows based on null checks (`health_record_id`, `clinic_key`, `record_date_key`, `age_group_key`).

#### `fact_disease_record`
- Grain: 1 row per disease record (`disease_record_id`).
- Source: `silver.disease_records`.
- Key columns: `health_record_id`, `disease_code`, `disease_name`.
- Materialization: view.

#### `fact_disease_record_valid` / `fact_disease_record_invalid`
- Split based on null checks (`disease_record_id`, `health_record_id`, `disease_code`).

#### `fact_inventory_import`
- Grain: 1 row per medicine import detail (`import_fact_id`).
- Source: `silver.medicine_import_details` (`status='active'`).
- Join keys: `date_key`, `clinic_key`, `medicine_key`, `lot_key`.
- Key metrics: `quantity_imported`, `initial_import_value`.
- Materialization: incremental merge.

#### `fact_inventory_import_valid` / `fact_inventory_import_invalid`
- `valid`: all primary keys non-null.
- `invalid`: error rows + `invalid_reason`.

#### `fact_inventory_export`
- Grain: 1 row per prescription medicine detail (`export_fact_id`).
- Source: `silver.prescription_medicine_details` + `silver.prescription_medicines`.
- Join keys: `date_key`, `clinic_key`, `medicine_key`, `lot_key`.
- Metric: `quantity_exported`.
- Materialization: incremental merge.

#### `fact_inventory_export_valid` / `fact_inventory_export_invalid`
- Split into valid/invalid rows based on null checks.

#### `fact_inventory_snapshot`
- Grain: 1 row per inventory lot (`snapshot_fact_id` = `medicine_import_detail_id`).
- Logic source:
  - Active imports from `silver.medicine_import_details`.
  - Minus total exports per lot from `fact_inventory_export`.
- Key metrics:
  - `current_quantity = quantity_imported - total_exported`.
  - `current_inventory_value = current_quantity * import_price`.
  - `days_to_expire`.
- Materialization: view.

#### `fact_inventory_snapshot_valid` / `fact_inventory_snapshot_invalid`
- `valid`: non-null keys + `current_quantity > 0`.
- `invalid`: null keys or `current_quantity <= 0`.

## 5) Gold Models (Marts/KPIs)

#### `gold_clinic_doctor_operational_efficiency_weekly`
- Grain: 1 row per clinic per week (`week_start_date_key`).
- Input: `fact_operational_clinic_bookings_valid`, `dim_date`, `dim_clinics`.
- KPIs: `completed_booking_count`, `avg_confirm_duration_sec`, `avg_consult_duration_sec`, `cancellation_rate`.

#### `gold_clinic_visits_doctor_monthly`
- Grain: 1 row per clinic per month.
- KPI: `number_booking`.
- Input: booking valid + dim date + dim clinic.

#### `gold_clinic_patient_visits_monthly`
- Grain: 1 row per clinic per month.
- KPIs:
  - `new_visits`: patients with their first visit in the month.
  - `returning_visits`: returning patients.
  - `retention_rate`.

#### `gold_prescription_value_daily`
- Grain: 1 row per clinic / doctor / room / day.
- KPIs: `prescription_count`, `total_value`, `avg_price_effective`.
- Pricing rule: prioritizes `price_included_vat`, falls back to `price`.

#### `gold_disease_top_by_clinic_age_weekly`
- Grain: 1 row per clinic / week / age_group / disease_code.
- KPIs: `total_occurrences`, `unique_records`.
- Input: `fact_health_record_valid` + `fact_disease_record_valid`.

#### `gold_best_selling_items`
- Grain: 1 row per clinic_name / medicine_name / year / month.
- KPIs: `total_quantity_exported`, `rank` by clinic-month.

#### `gold_low_stock_items`
- Grain: 1 row per clinic / medicine.
- KPIs: `total_current_quantity`, `items_sold_prev_month`, `is_low_stock`.
- Alert rule: current stock < previous month's sales.

#### `gold_near_expiry_drugs`
- Grain: 1 row per lot.
- KPIs/flags: `days_to_expire`, `urgent_level` (`red/yellow/green`).

#### `gold_slow_moving_items`
- Grain: 1 row per inventory lot.
- KPIs/flags: `days_in_stock`, `is_slow_moving`.
- Rule: lots with stock > 0 and no export transactions in the last 3 months.

#### `gold_visits_doctor_monthly`
- Grain: 1 row per doctor per month (legacy online flow).
- KPI: `number_booking`.
- Input: `rev_fact_bookings` + `rev_dim_doctors` + `dim_date`.

#### `rev_gold_revenue`
- Grain: 1 row per booking per source_type (`OFFLINE` + `ONLINE`).
- Purpose: unify online/offline revenue into a single mart.
- KPIs: `gross_revenue`, `amaz_revenue`, `partner_share`, `service_amount_only`, `medicine_amount_total`.
- Materialization: incremental merge.

## 6) Quick Reference for Data Analysts

### Business Questions and Recommended Models
- Clinic operational efficiency: `gold_clinic_doctor_operational_efficiency_weekly`.
- Visit volume by clinic per month: `gold_clinic_visits_doctor_monthly`.
- New vs. returning patients: `gold_clinic_patient_visits_monthly`.
- Prescription value by day: `gold_prescription_value_daily`.
- Top diseases by age group: `gold_disease_top_by_clinic_age_weekly`.
- Inventory risks: `gold_low_stock_items`, `gold_near_expiry_drugs`, `gold_slow_moving_items`, `gold_best_selling_items`.
- Consolidated online/offline revenue: `rev_gold_revenue`.

### When Tracing Back to Source Data
- Gold -> Platinum valid -> Platinum base -> Silver source.
- If a KPI looks abnormal, check the corresponding `*_invalid` model in the same domain to find the root cause.

## 7) Technical & Operational Assessment (DE View)

### Strengths
- Clear star schema structure (dim/fact).
- `valid/invalid` split in place for critical facts.
- Most large facts/gold models use incremental merge.

### Risks / Points to Watch
- Many models use `INT64 range` partition with range `20000101 -> 21000101, interval=100`.
- This configuration can generate a large number of partitions; monitor for BigQuery "possible partitions exceeding limit" errors.
- `fact_inventory_snapshot` is a `view` that joins `fact_inventory_export`; this may be costly to query at large data volumes.
- `gold_clinic_doctor_operational_efficiency_weekly` calculates `cancellation_rate` using condition `canceled_date_key=10101`; confirm this is the intended convention.
- The project currently lacks SLA, owner, and freshness expectation definitions per model in the schema docs.

### Suggested Next Priorities
- Switch to `DATE` column partitioning or month-key partitioning to reduce partition limit risk.
- Add `description` for models and columns in `schema.yml` for richer dbt docs.
- Set explicit test severity policies (`error/warn`) based on business impact.
- Add exposure/dashboard mapping to trace from BI back to models.

## 8) Common Commands

- Build platinum:
  - `dbt build --target platinum --select path:models/platinum --profiles-dir . --project-dir .`
- Build gold:
  - `dbt build --target gold --select path:models/gold --profiles-dir . --project-dir .`
- Build a single model and its downstream dependencies:
  - `dbt build --select +gold_clinic_patient_visits_monthly --profiles-dir . --project-dir .`

## 9) Related Files
- `dbt/dbt_project.yml`
- `dbt/profiles.yml`
- `dbt/models/platinum/schema.yml`
- `dbt/models/gold/schema.yml`
- `dbt/models/sources/src_silver.yml`
- `dbt/models/sources/src_platinum.yml`
