# Star Schema (dbt)

Tài liệu này tóm tắt star schema cho các model chính trong `dbt/models` (đặc biệt các model gold/platinum bạn đang làm).

## 1) Platinum Star Schemas

```mermaid
erDiagram
    FACT_OPERATIONAL_CLINIC_BOOKINGS {
      string booking_id PK
      int clinic_key FK
      int doctor_key FK
      int patient_key FK
      int created_date_key FK
      int from_date_key FK
      int confirmed_date_key FK
      int finished_date_key FK
      int canceled_date_key FK
      int room_id
    }

    DIM_CLINICS {
      int clinic_key PK
      int clinic_id
      int admin_user_id
      string clinic_name
    }
    DIM_CLINIC_DOCTORS {
      int doctor_key PK
      int doctor_id
      int clinic_doctor_id
      string doctor_name
    }
    DIM_CLINIC_PATIENTS {
      int patient_key PK
      int patient_id
      int patient_user_id
      int clinic_patient_id
    }
    DIM_DATE {
      int date_key PK
      date full_date
      int month_start_date_key
    }
    DIM_CLINIC_ROOMS {
      int room_id PK
      int clinic_key
      int doctor_key
      string room_name
    }

    FACT_OPERATIONAL_CLINIC_BOOKINGS }o--|| DIM_CLINICS : clinic_key
    FACT_OPERATIONAL_CLINIC_BOOKINGS }o--|| DIM_CLINIC_DOCTORS : doctor_key
    FACT_OPERATIONAL_CLINIC_BOOKINGS }o--|| DIM_CLINIC_PATIENTS : patient_key
    FACT_OPERATIONAL_CLINIC_BOOKINGS }o--|| DIM_DATE : from_date_key
    FACT_OPERATIONAL_CLINIC_BOOKINGS }o--o{ DIM_CLINIC_ROOMS : room_id
```

```mermaid
erDiagram
    FACT_PRESCRIPTION {
      string prescription_id PK
      int clinic_key FK
      int doctor_key FK
      int prescription_date_key FK
      int room_id
      numeric price
      numeric price_included_vat
    }

    DIM_CLINICS {
      int clinic_key PK
      string clinic_name
    }
    DIM_CLINIC_DOCTORS {
      int doctor_key PK
      string doctor_name
    }
    DIM_DATE {
      int date_key PK
    }
    DIM_CLINIC_ROOMS {
      int room_id PK
      int clinic_key
      int doctor_key
    }

    FACT_PRESCRIPTION }o--|| DIM_CLINICS : clinic_key
    FACT_PRESCRIPTION }o--|| DIM_CLINIC_DOCTORS : doctor_key
    FACT_PRESCRIPTION }o--|| DIM_DATE : prescription_date_key
    FACT_PRESCRIPTION }o--o{ DIM_CLINIC_ROOMS : room_id
```

```mermaid
erDiagram
    FACT_INVENTORY_IMPORT {
      string import_fact_id PK
      int date_key FK
      int clinic_key FK
      int medicine_key FK
      int lot_key FK
      int quantity_imported
    }
    FACT_INVENTORY_EXPORT {
      string export_fact_id PK
      int date_key FK
      int clinic_key FK
      int medicine_key FK
      int lot_key FK
      int quantity_exported
    }
    FACT_INVENTORY_SNAPSHOT {
      string snapshot_fact_id PK
      int clinic_key FK
      int medicine_key FK
      int lot_key FK
      int current_quantity
    }

    DIM_CLINICS {
      int clinic_key PK
    }
    DIM_MEDICINES {
      int medicine_key PK
    }
    DIM_MEDICINES_LOT {
      int lot_key PK
      int medicine_import_detail_id
    }
    DIM_DATE {
      int date_key PK
    }

    FACT_INVENTORY_IMPORT }o--|| DIM_DATE : date_key
    FACT_INVENTORY_IMPORT }o--|| DIM_CLINICS : clinic_key
    FACT_INVENTORY_IMPORT }o--|| DIM_MEDICINES : medicine_key
    FACT_INVENTORY_IMPORT }o--|| DIM_MEDICINES_LOT : lot_key

    FACT_INVENTORY_EXPORT }o--|| DIM_DATE : date_key
    FACT_INVENTORY_EXPORT }o--|| DIM_CLINICS : clinic_key
    FACT_INVENTORY_EXPORT }o--|| DIM_MEDICINES : medicine_key
    FACT_INVENTORY_EXPORT }o--|| DIM_MEDICINES_LOT : lot_key

    FACT_INVENTORY_SNAPSHOT }o--|| DIM_CLINICS : clinic_key
    FACT_INVENTORY_SNAPSHOT }o--|| DIM_MEDICINES : medicine_key
    FACT_INVENTORY_SNAPSHOT }o--|| DIM_MEDICINES_LOT : lot_key
```

```mermaid
erDiagram
    FACT_HEALTH_RECORD {
      string health_record_id PK
      int clinic_key FK
      int record_date_key FK
      int age_group_key
    }
    FACT_DISEASE_RECORD {
      string disease_record_id PK
      string health_record_id FK
      string disease_code
    }
    DIM_CLINICS {
      int clinic_key PK
    }
    DIM_DATE {
      int date_key PK
    }

    FACT_HEALTH_RECORD }o--|| DIM_CLINICS : clinic_key
    FACT_HEALTH_RECORD }o--|| DIM_DATE : record_date_key
    FACT_DISEASE_RECORD }o--|| FACT_HEALTH_RECORD : health_record_id
```

```mermaid
erDiagram
    REV_FACT_BOOKINGS {
      int id PK
      int doctor_key FK
      int patient_key FK
      int booking_date_key FK
    }
    REV_DIM_DOCTORS {
      int doctor_key PK
      int doctor_id
    }
    REV_DIM_PATIENTS {
      int patient_key PK
      int patient_id
    }
    DIM_DATE {
      int date_key PK
    }

    REV_FACT_BOOKINGS }o--|| REV_DIM_DOCTORS : doctor_key
    REV_FACT_BOOKINGS }o--|| REV_DIM_PATIENTS : patient_key
    REV_FACT_BOOKINGS }o--|| DIM_DATE : booking_date_key
```

## 2) Gold Models Mapped To Stars

```mermaid
flowchart LR
    subgraph STAR_OP["Star: Operational Clinic Booking"]
      FOP["fact_operational_clinic_bookings_valid"]
      DCL["dim_clinics"]
      DDT["dim_date"]
      DCD["dim_clinic_doctors"]
      DCP["dim_clinic_patients"]
    end

    subgraph STAR_RX["Star: Prescription"]
      FPR["fact_prescription_valid"]
      DCL2["dim_clinics"]
      DCD2["dim_clinic_doctors"]
    end

    G1["gold_clinic_doctor_operational_efficiency_weekly"]
    G2["gold_clinic_patient_visits_monthly"]
    G3["gold_clinic_visits_doctor_monthly"]
    G4["gold_prescription_value_daily"]

    FOP --> G1
    DCL --> G1
    DDT --> G1

    FOP --> G2
    DCL --> G2
    DDT --> G2
    DCP --> G2

    FOP --> G3
    DCL --> G3
    DDT --> G3

    FPR --> G4
    DCL2 --> G4
    DCD2 --> G4
```

## 3) Ghi chú nhanh

- `fact_*_valid` là lớp fact đã lọc chất lượng dữ liệu và là nguồn chính cho gold.
- `clinic_key` ở các fact clinic đang join qua `dim_clinics.admin_user_id` (theo chuẩn bạn đang áp dụng).
- Nhánh `rev_*` là nhánh revenue online riêng, dùng `rev_dim_doctors` và `rev_dim_patients`.

