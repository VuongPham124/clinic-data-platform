## Data Platform Demo (Snapshot + CDC + Curated + Master + dbt + Monitoring)

Repository này chứa toàn bộ stack data pipeline cho bài toán phòng khám:
- Ingest dữ liệu từ Postgres (full snapshot + CDC).
- Chuẩn hóa dữ liệu lên `silver`.
- Áp rule chất lượng dữ liệu sang `silver_curated`.
- Sinh bảng master (patient ID, drug code).
- Xây mô hình `platinum`/`gold` bằng dbt.
- Giám sát vận hành bằng BigQuery SQL + dashboard + alert streaming.

---

## 1) Kiến trúc tổng thể

### 1.1 Luồng batch snapshot
`Postgres -> GCS raw -> Spark raw_to_staging -> GCS staging -> BigQuery silver -> curated/master/dbt`

Thành phần chính:
- `pipelines/postgres_to_gcs.py`
- `pipelines/raw_to_staging.py`
- `pipelines/staging_to_silver.py`
- `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`
- `pipelines/gen_master/*`
- `dbt/models/platinum/*`, `dbt/models/gold/*`

### 1.2 Luồng CDC daily
`Datastream AVRO -> Spark normalize -> GCS parquet stage -> BigQuery stage -> MERGE silver -> curated/master/dbt`

Thành phần chính:
- `pipelines/cdc/normalize/*`
- `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`

### 1.3 Orchestration
Airflow/Composer DAGs trong `dags/` điều phối:
- `main_full_batch_flow`
- `main_cdc_flow`
- các DAG con (`full_snapshot_batch_pipeline`, `cdc_daily_to_silver`, `silver_to_silver_curated_dq_v2_3`, `gen_master_parallel`, `dbt_platinum_gold_build_test`)

---

## 2) Cấu trúc thư mục

- `dags/`: Airflow DAGs orchestration.
- `pipelines/`: Spark/Python/Bash ETL scripts.
- `contract/`: Data contract/rules JSON.
- `dbt/`: dbt project (`platinum`, `gold`, `sources`).
- `monitoring/`: SQL monitors, runbook, dashboards, exporter.
- `streaming_alert/`: realtime alert pipeline (Debezium -> Dataflow -> Pub/Sub -> Cloud Run).
- `bi/`: docker-compose cho Metabase.
- `poc/`: PoC utilities.

---

## 3) DAG inventory (runtime chính)

### 3.1 Entry DAGs
- `main_full_batch_flow`: full snapshot end-to-end.
- `main_cdc_flow`: CDC daily end-to-end.

### 3.2 Pipeline DAGs
- `full_snapshot_batch_pipeline`: Postgres snapshot -> staging -> silver.
- `cdc_daily_to_silver`: normalize CDC + merge silver.
- `silver_to_silver_curated_dq_v2_3`: curated DQ.
- `gen_master_parallel`: chạy 2 job master.
- `dbt_platinum_gold_build_test`: build/test dbt platinum rồi gold.
- `dbt_platinum_gold_full_refresh`: full-refresh dbt thủ công (DAG bổ sung).

Tham khảo chi tiết:
- `dags/README.md`

---

## 4) Pipelines chi tiết

### 4.1 Snapshot ingest
- `pipelines/postgres_to_gcs.py`: extract snapshot từ Postgres -> `gs://.../snapshot/...`.
- `pipelines/raw_to_staging.py`: Spark cast/validate/dedup -> parquet staging + quarantine.
- `pipelines/staging_to_silver.py`: load parquet lên BigQuery `silver`.

### 4.2 CDC normalize + merge
- `pipelines/cdc/normalize/main.py`: entry Spark normalize.
- `pipelines/cdc/normalize/table_processor.py`: xử lý từng bảng.
- `pipelines/cdc/normalize/cdc_normalizer.py`: normalize metadata CDC (`__op`, `__lsn_num`, `__commit_ts`, ...).
- `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`: load stage + MERGE sang `silver`, cập nhật watermark.

### 4.3 Curated DQ
- `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`:
  - apply rules JSON + null policy + DQ checks.
  - split `good`/`bad` sang curated/quarantine.
  - ghi metrics vào `silver_curated_dq.dq_run_metrics`.

### 4.4 Master generation
- `pipelines/gen_master/gen_master_patient_id_script_patched.py`
- `pipelines/gen_master/gen_master_drug_code_medicines_script_v2.py`

Tham khảo chi tiết:
- `pipelines/README.md`

---

## 5) dbt layer

Project:
- `dbt/dbt_project.yml`
- `dbt/profiles.yml`

Nguồn dữ liệu:
- `dbt/models/sources/src_silver.yml`
- `dbt/models/sources/src_platinum.yml`

Model domains:
- `dbt/models/platinum/dims/*`
- `dbt/models/platinum/facts/*`
- `dbt/models/gold/*`

Test schemas:
- `dbt/models/platinum/schema.yml`
- `dbt/models/gold/schema.yml`

Tham khảo chi tiết:
- `dbt/README.md`

---

## 6) Monitoring & Observability

`monitoring/sql/*` chứa bộ query đo:
- watermark health, freshness, duplicate PK, quality ratio,
- BigQuery/Composer/Scheduler/Datastream/GCS health,
- critical DAG SLO.

Artifacts:
- `monitoring/dashboard/*`
- `monitoring/exporter/*` (đẩy custom metrics lên Cloud Monitoring).

Docs:
- `monitoring/README.md`
- `monitoring/RUNBOOK.md`
- `monitoring/METRICS_GUIDE.md`

---

## 7) Streaming alert (near realtime)

Module `streaming_alert/`:
- Debezium CDC (`public.clinic_bookings`) -> Pub/Sub raw.
- Dataflow detect rules theo window 1 phút.
- Pub/Sub push -> Cloud Run orchestrator.
- Firestore state + BigQuery `ops_monitor.alert_history`.

Doc:
- `streaming_alert/README.md`

---

## 8) Tối ưu đã áp dụng gần đây

Tổng hợp cập nhật tối ưu:
- `OPTIMIZATION_README.md`

Các điểm chính:
- Curated DQ: AQE + adaptive repartition + metrics path gọn hơn.
- CDC merge: run-scoped temp source (`run_scoped_source`, `run_scoped_latest`) giảm scan.
- dbt platinum/gold: nhiều model chuyển sang incremental + merge + partition/cluster.
- Bổ sung DAG full-refresh dbt: `dags/dbt_platinum_gold_full_refresh_dag.py`.

Lưu ý BigQuery partition:
- Int range partition đã chỉnh `interval=100` để tránh lỗi vượt giới hạn partition count.

---

## 9) Cách chạy nhanh

### 9.1 Airflow / Composer
- Full flow snapshot: trigger `main_full_batch_flow`.
- Full flow CDC: trigger `main_cdc_flow`.
- dbt full refresh thủ công: trigger `dbt_platinum_gold_full_refresh`.

### 9.2 dbt local
Trong thư mục `dbt/`:

```bash
dbt debug --profiles-dir . --project-dir .
dbt build --target platinum --select path:models/platinum --profiles-dir . --project-dir .
dbt build --target gold --select path:models/gold --profiles-dir . --project-dir .
```

Full-refresh:

```bash
dbt build --full-refresh --target platinum --select path:models/platinum --profiles-dir . --project-dir .
dbt build --full-refresh --target gold --select path:models/gold --profiles-dir . --project-dir .
```

### 9.3 CDC merge manual (table-specific)
`ONLY_TABLE` mode có sẵn trong merge script:

```bash
./run_query_merge_v2.updated3.sh <SRC_DATE> <RUN_ID> contract/cdc_staging_contract_v1.json public_users
```

---

## 10) Environment variables quan trọng

### Airflow / DAG runtime
- `COMPOSER_BUCKET` / `COMPOSER_GCS_BUCKET` / `DAGS_BUCKET` / `GCS_BUCKET`
- `TEMP_GCS_BUCKET`
- `BIGQUERY_CONNECTOR_JAR_URI` (allowlist trong `gen_master_parallel_dag.py`)

### dbt
- `DBT_GCP_PROJECT`
- `DBT_BQ_LOCATION`
- `DBT_DATASET_PLATINUM`
- `DBT_DATASET_GOLD`
- `DBT_THREADS`
- `DBT_TARGET`

### Merge script
- `PROJECT_ID`
- `LOCATION`
- `STAGING_BUCKET`

---

## 11) Data contracts & quality

Contract files:
- `contract/staging_contract_snapshot_v1.json`
- `contract/cdc_staging_contract_v1.json`
- `contract/cdc_merge_contract_v1.json`
- `contract/silver_curated_business_rules_v1.json`

Các file này quyết định:
- schema cast, PK/required validations,
- mapping timestamp/decimal,
- DQ checks và null policies.

---

## 12) Testing strategy

Tài liệu kiểm thử hệ thống + dữ liệu:
- `TESTING_README.md`
- `docs/DATA_PLATFORM_RISK_AND_OPERATIONS_GUIDE.md` (incident playbook, backfill, trade-off, technology fit by data scale)

Bao gồm:
- DAG parse/dependency tests
- snapshot/CDC E2E
- idempotency/rerun
- DQ/quarantine checks
- dbt build/test gate
- release go/no-go criteria

---

## 13) BI stack

`bi/docker-compose.yml` cung cấp stack Metabase + Postgres metadata backend để phục vụ dashboard nội bộ.

---

## 14) Known caveats

- Một số script chứa block comment lịch sử để truy vết logic cũ.
- Một số giá trị project/bucket/cluster vẫn hard-code trong DAG/script, nên chuẩn hóa qua env trước khi prod hóa đa môi trường.
- Khi đổi materialization sang incremental, cần full-refresh bootstrap một lần đầu.

---

## 15) License

`LICENSE` tại root repository.

---

## 16) Technical & Operational Assessment (Data Engineering View)

Section này tập trung vào: kiến trúc kỹ thuật, độ ổn định vận hành, rủi ro production, và ưu tiên cải tiến theo từng module.

### 16.1 `dags/` (Airflow orchestration)
Technical assessment:
- Thiết kế rõ luồng điều phối qua `main_full_batch_flow` và `main_cdc_flow`.
- DAG decomposition hợp lý: ingest, curated, master, dbt tách biệt.
- Có DAG riêng cho dbt full-refresh giúp thao tác vận hành an toàn hơn.

Operational assessment:
- `max_active_runs=1` ở các DAG chính giúp tránh race condition.
- Trigger chain `wait_for_completion=True` đơn giản, dễ quan sát nhưng tăng end-to-end latency.
- Một số config còn hard-code (`cluster`, `project`, bucket), chưa tối ưu đa môi trường.

Risk level: Medium  
Priority:
- Chuẩn hóa config bằng env/Variables.
- Thêm SLA timeout và alert policy mức task.

### 16.2 `pipelines/postgres_to_gcs.py` + `raw_to_staging.py` + `staging_to_silver.py` (snapshot flow)
Technical assessment:
- Luồng chuẩn ETL 3 tầng rõ ràng, có history artifacts.
- `raw_to_staging` đã có kiểm tra chất lượng và quarantine.
- Chưa có table-level parallelism rõ rệt ở snapshot extract/load.

Operational assessment:
- Dễ replay theo `LOAD_DATE`/`RUN_ID`.
- Khả năng kéo dài runtime khi volume tăng do xử lý tuần tự.
- Cần kiểm soát small files và memory pressure khi chunk lớn.

Risk level: Medium  
Priority:
- Parallelize theo bảng và tuning chunk adaptive.
- Bổ sung benchmark cố định theo table class (small/medium/large).

### 16.3 `pipelines/cdc/normalize/*` (CDC normalize)
Technical assessment:
- Contract-driven normalize tốt, có metadata CDC đầy đủ.
- Phân tách module hợp lý (`cdc_normalizer`, `schema_applier`, `table_processor`, `io_utils`).
- Có manifest output giúp truy vết run tốt.

Operational assessment:
- Độ tin cậy cao cho rerun theo `source_date_local`/`run_id`.
- Cần giảm action dư (count/debug) để ổn định chi phí Dataproc.
- Phụ thuộc chặt vào chất lượng AVRO metadata đầu vào.

Risk level: Medium  
Priority:
- Guard debug metrics mặc định OFF.
- Tối ưu file sizing/coalesce strategy theo volume.

### 16.4 `pipelines/cdc/merge/run_query_merge_v2.updated3.sh` (BigQuery merge)
Technical assessment:
- Đã có watermark 2 nhánh (`real lsn` + `inferred`), idempotent tốt.
- Đã tối ưu bằng run-scoped source + dedupe temp table trước MERGE.
- SQL dynamic phức tạp nhưng có validate contract và identifier safety.

Operational assessment:
- Là module critical nhất về chi phí BigQuery (slot-ms/bytes).
- Có `ONLY_TABLE` rất hữu ích cho incident rerun.
- Cần governance schema/partition stage-silver để giữ hiệu năng ổn định dài hạn.

Risk level: Medium-High  
Priority:
- Theo dõi bytes processed theo bảng mỗi ngày.
- Bổ sung regression checks cho watermark monotonicity.

### 16.5 `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`
Technical assessment:
- Rule engine rõ ràng: null policy + dq checks + quarantine + metrics.
- Đã có adaptive partitions và AQE configs.
- Rule alias resolution giúp giảm mismatch contract/rules.

Operational assessment:
- Dễ audit nhờ `dq_run_metrics` và `dq_reason`.
- Khả năng mở rộng phụ thuộc thiết kế rules JSON theo bảng.
- Nên quản lý rules versioning chặt (change log + approval flow).

Risk level: Low-Medium  
Priority:
- Thiết lập ngưỡng alert theo bad ratio/table.
- Tách bảng nặng thành job độc lập khi runtime tăng.

### 16.6 `pipelines/gen_master/*` (master generation)
Technical assessment:
- `gen_master_drug_code_medicines_script_v2.py`: đã loại bỏ luồng driver-wide `toPandas`, dùng `mapInPandas`.
- `gen_master_patient_id_script_patched.py`: hiện runtime vẫn theo Python UDF path (chưa fully SQL-expression path).
- Logic business khá dày, cần module hóa mạnh hơn để dễ test.

Operational assessment:
- Drug module đã cải thiện scalability đáng kể.
- Patient module có rủi ro CPU overhead khi data tăng.
- Hai task master chạy song song có thể tranh tài nguyên trên cluster nhỏ.

Risk level:
- Drug: Low-Medium
- Patient: Medium-High

Priority:
- Chuyển patient ID sang Spark native expression path.
- Thiết lập resource profile riêng theo từng job master.

### 16.7 `dbt/models/platinum/*`
Technical assessment:
- Dims/facts tách lớp rõ, `valid/invalid` pattern tốt cho data quality.
- Nhiều fact lớn đã chuyển incremental+merge + partition/cluster.
- Đã xử lý lỗi partition count bằng `interval=100` cho int range.

Operational assessment:
- Build time cải thiện rõ sau incremental migration.
- Cần full-refresh bootstrap khi đổi materialization.
- Một số model vẫn phụ thuộc logic legacy/rev branch, cần governance dần.

Risk level: Medium  
Priority:
- Chuẩn hóa incremental window theo domain.
- Thiết lập cadence full-refresh định kỳ cho model có late-arriving cao.

### 16.8 `dbt/models/gold/*`
Technical assessment:
- Gold marts bao phủ KPI chính (operational, inventory, disease, revenue).
- Đã incremental hóa phần lớn model time-series.
- Giữ `gold_clinic_patient_visits_monthly` dạng table là quyết định đúng về correctness.

Operational assessment:
- Phù hợp phục vụ BI batch daily.
- Cần theo dõi data drift business logic (không chỉ schema tests).
- Nên có contract test cho KPI quan trọng (retention, cancellation rate, revenue).

Risk level: Medium  
Priority:
- Bổ sung business assertions theo domain.
- Theo dõi freshness end-to-end từ silver -> gold.

### 16.9 `monitoring/*`
Technical assessment:
- Bộ SQL monitor đầy đủ từ watermark, freshness, duplicates đến job health.
- Có runbook và dashboard templates.
- Có exporter sang custom metrics để tích hợp Cloud Monitoring.

Operational assessment:
- Mức sẵn sàng vận hành cao nếu Scheduled Queries và Log Sinks được cấu hình đúng.
- Chất lượng monitor phụ thuộc dữ liệu log export đầy đủ.
- Cần ownership rõ cho alert triage và on-call action.

Risk level: Low-Medium  
Priority:
- Chuẩn hóa naming output tables và retention policy.
- Drill alert định kỳ (fire drill) để kiểm chứng runbook.

### 16.10 `streaming_alert/*` (real-time alert)
Technical assessment:
- Kiến trúc event-driven hợp lý: Debezium -> Dataflow -> Pub/Sub -> Cloud Run.
- Có dedup + cooldown + history audit trong BigQuery.
- Phù hợp cho near realtime alerting use cases.

Operational assessment:
- Nhiều moving parts (Pub/Sub, Run, Dataflow, Firestore) nên cần SRE discipline.
- Rủi ro cấu hình IAM/secret/runtime dependency cao hơn batch pipeline.
- Cần theo dõi DLQ, retry, push endpoint auth thường xuyên.

Risk level: Medium-High  
Priority:
- Thiết lập SLO riêng cho alert latency và delivery success.
- Bổ sung dead-letter strategy + replay procedure rõ ràng.

### 16.11 `contract/*` (data contracts)
Technical assessment:
- Là backbone cho normalize/merge/curated.
- Có validate JSON và identifier safety trong merge script.

Operational assessment:
- Contract change là high-impact, cần release process có review/approval.
- Cần lưu version và mapping tương ứng từng run để audit.

Risk level: Medium  
Priority:
- Enforce contract CI checks trước deploy.
- Lưu metadata contract version vào run artifacts.

---

## 17) Suggested Operating Model for Data Engineering Team

### Ownership split
- Platform/DE: DAG + Spark + merge runtime + cost/perf.
- Analytics Engineering: dbt models/tests + semantic correctness.
- Data Quality/Ops: monitors, alert routing, incident playbook.

### Weekly review pack (minimum)
- DAG p50/p95 duration.
- BigQuery bytes processed + slot-ms theo module.
- Quarantine ratio và duplicate PK trend.
- Freshness lag silver/platinum/gold.
- Top 5 costly jobs + optimization action items.
