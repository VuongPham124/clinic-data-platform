## Data Platform Demo (Snapshot + CDC + Manual File Upload + Curated + Master + dbt + Monitoring)

This repository contains the full data pipeline stack for a clinic management use case:
- Ingest data from Postgres (full snapshot + CDC).
- Normalize data into `silver`.
- Apply data quality rules into `silver_curated`.
- Generate master tables (patient ID, drug code).
- Build `platinum`/`gold` models with dbt.
- Upload manual files (Excel/CSV) to GCS to update reference data, inventory, and patient records.
- Operational monitoring via BigQuery SQL + dashboard + streaming alerts.

---

## 1) Overall Architecture

### 1.1 Batch Snapshot Flow
`Postgres -> GCS raw -> Spark raw_to_staging -> GCS staging -> BigQuery silver -> curated/master/dbt`

Key components:
- `pipelines/postgres_to_gcs.py`
- `pipelines/raw_to_staging.py`
- `pipelines/staging_to_silver.py`
- `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`
- `pipelines/gen_master/*`
- `dbt/models/platinum/*`, `dbt/models/gold/*`

### 1.2 Daily CDC Flow
`Datastream AVRO -> Spark normalize -> GCS parquet stage -> BigQuery stage -> MERGE silver -> curated/master/dbt`

Key components:
- `pipelines/cdc/normalize/*`
- `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`

### 1.3 Manual File Upload Flow
`User uploads Excel/CSV -> GCS manual_file/<subfolder>/ -> Cloud Run transform -> BigQuery manual (staging) -> MERGE/INSERT silver`

Key components:
- Cloud Run service `amaz-file-etl` — receives GCS events via Pub/Sub, identifies file type by subfolder, cleans and loads into staging
- BigQuery `manual.__pipeline_config` — config table defining query and processing order for each file type
- BigQuery `silver.*` — destination tables after the pipeline completes

Routing is based on the subfolder name in `gs://amaz-raw/manual_file/<file_key>/`:

| `file_key` | Destination Table |
|---|---|
| `medicine_categories` | `silver.public_medicines` |
| `inventory` | `silver.public_medicine_imports` → `silver.public_medicine_import_details` |
| `patients` | `silver.public_users` → `silver.public_patients` |

Adding a new file type requires no redeployment — just INSERT a config row into `__pipeline_config`.

For detailed deploy, upload, and extension instructions:
- `manual_file/README.md`

### 1.4 Orchestration
Airflow/Composer DAGs in `dags/` orchestrate:
- `main_full_batch_flow`
- `main_cdc_flow`
- Child DAGs (`full_snapshot_batch_pipeline`, `cdc_daily_to_silver`, `silver_to_silver_curated_dq_v2_3`, `gen_master_parallel`, `dbt_platinum_gold_build_test`)

---

## 2) Directory Structure

- `dags/`: Airflow DAGs for orchestration.
- `pipelines/`: Spark/Python/Bash ETL scripts.
- `manual_file/`: Cloud Run service handling manual file uploads (`main.py`, `Dockerfile`, `requirements.txt`).
- `contract/`: Data contract/rules JSON files.
- `dbt/`: dbt project (`platinum`, `gold`, `sources`).
- `monitoring/`: SQL monitors, runbook, dashboards, exporter.
- `streaming_alert/`: Real-time alert pipeline (Debezium -> Dataflow -> Pub/Sub -> Cloud Run).
- `poc/`: PoC utilities.

---

## 3) DAG Inventory (Main Runtime)

### 3.1 Entry DAGs
- `main_full_batch_flow`: Full snapshot end-to-end.
- `main_cdc_flow`: CDC daily end-to-end.

### 3.2 Pipeline DAGs
- `full_snapshot_batch_pipeline`: Postgres snapshot -> staging -> silver.
- `cdc_daily_to_silver`: Normalize CDC + merge silver.
- `silver_to_silver_curated_dq_v2_3`: Curated DQ.
- `gen_master_parallel`: Runs 2 master jobs in parallel.
- `dbt_platinum_gold_build_test`: Build/test dbt platinum then gold.
- `dbt_platinum_gold_full_refresh`: Manual dbt full-refresh (supplementary DAG).

For detailed reference:
- `dags/README.md`

---

## 4) Pipeline Details

### 4.1 Snapshot Ingest
- `pipelines/postgres_to_gcs.py`: Extract snapshot from Postgres -> `gs://.../snapshot/...`.
- `pipelines/raw_to_staging.py`: Spark cast/validate/dedup -> parquet staging + quarantine.
- `pipelines/staging_to_silver.py`: Load parquet into BigQuery `silver`.

### 4.2 CDC Normalize + Merge
- `pipelines/cdc/normalize/main.py`: Spark normalize entry point.
- `pipelines/cdc/normalize/table_processor.py`: Processes each table.
- `pipelines/cdc/normalize/cdc_normalizer.py`: Normalizes CDC metadata (`__op`, `__lsn_num`, `__commit_ts`, ...).
- `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`: Load stage + MERGE into `silver`, update watermarks.

### 4.3 Curated DQ
- `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`:
  - Applies rules JSON + null policy + DQ checks.
  - Splits `good`/`bad` rows into curated/quarantine.
  - Writes metrics to `silver_curated_dq.dq_run_metrics`.

### 4.4 Master Generation
- `pipelines/gen_master/gen_master_patient_id_script_patched.py`
- `pipelines/gen_master/gen_master_drug_code_medicines_script_v2.py`

For detailed reference:
- `pipelines/README.md`

### 4.5 Manual File ETL
- `manual_file/main.py`: Cloud Run handler — receives Pub/Sub event from GCS, parses subfolder to determine `file_key`, cleans headers, loads into BigQuery `manual.<file_key>` (staging), then runs queries from `__pipeline_config` in step order.
- `manual_file/query.sql`: Reference query for pipeline config setup.

For detailed reference:
- `manual_file/README.md`

---

## 5) dbt Layer

Project:
- `dbt/dbt_project.yml`
- `dbt/profiles.yml`

Data sources:
- `dbt/models/sources/src_silver.yml`
- `dbt/models/sources/src_platinum.yml`

Model domains:
- `dbt/models/platinum/dims/*`
- `dbt/models/platinum/facts/*`
- `dbt/models/gold/*`

Test schemas:
- `dbt/models/platinum/schema.yml`
- `dbt/models/gold/schema.yml`

For detailed reference:
- `dbt/README.md`

---

## 6) Monitoring & Observability

`monitoring/sql/*` contains query sets measuring:
- Watermark health, freshness, duplicate PK, quality ratio,
- BigQuery/Composer/Scheduler/Datastream/GCS health,
- Critical DAG SLOs.

Artifacts:
- `monitoring/dashboard/*`
- `monitoring/exporter/*` (pushes custom metrics to Cloud Monitoring).

Docs:
- `monitoring/README.md`
- `monitoring/RUNBOOK.md`
- `monitoring/METRICS_GUIDE.md`

---

## 7) Streaming Alert (Near Real-time)

Module `streaming_alert/`:
- Debezium CDC (`public.clinic_bookings`) -> raw Pub/Sub.
- Dataflow detects rules over 1-minute windows.
- Pub/Sub push -> Cloud Run orchestrator.
- Firestore state + BigQuery `ops_monitor.alert_history`.

Doc:
- `streaming_alert/README.md`

---

## 8) Recent Optimizations Applied

Optimization summary:
- `OPTIMIZATION_README.md`



Key highlights:
- Curated DQ: AQE + adaptive repartition + leaner metrics path.
- CDC merge: run-scoped temp source (`run_scoped_source`, `run_scoped_latest`) to reduce scan.
- dbt platinum/gold: many models migrated to incremental + merge + partition/cluster.
- New dbt full-refresh DAG added: `dags/dbt_platinum_gold_full_refresh_dag.py`.

BigQuery partition note:
- Int range partition interval changed to `interval=100` to avoid exceeding partition count limits.

---

## 9) Quick Start

### 9.1 Airflow / Composer
- Full snapshot flow: trigger `main_full_batch_flow`.
- Full CDC flow: trigger `main_cdc_flow`.
- Manual dbt full refresh: trigger `dbt_platinum_gold_full_refresh`.

### 9.2 dbt Local
From the `dbt/` directory:

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

### 9.3 Manual File Upload
Upload Excel files to the correct GCS subfolder (PowerShell):

```powershell
$ts = Get-Date -Format "yyyy/MM/dd/HH/mm"
gcloud storage cp "Medicine Categories.xlsx" gs://amaz-raw/manual_file/medicine_categories/$ts/
gcloud storage cp "Medicines.xlsx" gs://amaz-raw/manual_file/inventory/$ts/
gcloud storage cp "Patients.xlsx" gs://amaz-raw/manual_file/patients/$ts/
```

Cloud Run will auto-trigger via Pub/Sub once the file appears on GCS. For detailed deploy and template extension instructions:
- `manual_file/README.md`

### 9.4 Manual CDC Merge (Table-Specific)
`ONLY_TABLE` mode is available in the merge script:

```bash
./run_query_merge_v2.updated3.sh <SRC_DATE> <RUN_ID> contract/cdc_staging_contract_v1.json public_users
```

---

## 10) Key Environment Variables

### Airflow / DAG Runtime
- `COMPOSER_BUCKET` / `COMPOSER_GCS_BUCKET` / `DAGS_BUCKET` / `GCS_BUCKET`
- `TEMP_GCS_BUCKET`
- `BIGQUERY_CONNECTOR_JAR_URI` (allowlist in `gen_master_parallel_dag.py`)

### dbt
- `DBT_GCP_PROJECT`
- `DBT_BQ_LOCATION`
- `DBT_DATASET_PLATINUM`
- `DBT_DATASET_GOLD`
- `DBT_THREADS`
- `DBT_TARGET`

### Cloud Run — amaz-file-etl
- `PROJECT_ID`
- `BQ_DATASET` (staging dataset, default `manual`)
- `RAW_BUCKET`
- `STAGING_BUCKET`
- `BQ_SILVER_DATASET` (default `silver`)
- `MANUAL_FILE_PREFIX` (default `manual_file/`)

### Merge Script
- `PROJECT_ID`
- `LOCATION`
- `STAGING_BUCKET`

---

## 11) Data Contracts & Quality

Contract files:
- `contract/staging_contract_snapshot_v1.json`
- `contract/cdc_staging_contract_v1.json`
- `contract/cdc_merge_contract_v1.json`
- `contract/silver_curated_business_rules_v1.json`

These files govern:
- Schema casting, PK/required validations,
- Timestamp/decimal mapping,
- DQ checks and null policies.

---

## 12) Testing Strategy

System and data testing documentation:
- `TESTING_README.md`
- `docs/DATA_PLATFORM_RISK_AND_OPERATIONS_GUIDE.md` (incident playbook, backfill, trade-offs, technology fit by data scale)

Covers:
- DAG parse/dependency tests
- Snapshot/CDC end-to-end
- Idempotency/rerun
- DQ/quarantine checks
- dbt build/test gate
- Release go/no-go criteria

---

## 13) BI Stack

`bi/docker-compose.yml` provides a Metabase + Postgres metadata backend stack for internal dashboards.

---

## 14) Known Caveats

- Some scripts contain historical comment blocks to trace legacy logic.
- Some project/bucket/cluster values are still hard-coded in DAGs/scripts — standardize via env vars before multi-environment production rollout.
- When switching to incremental materialization, a one-time full-refresh bootstrap is required.

---

## 15) License

`LICENSE` at the repository root.

---

## 16) Technical & Operational Assessment (Data Engineering View)

This section covers: technical architecture, operational stability, production risks, and improvement priorities by module.

### 16.1 `dags/` (Airflow Orchestration)
Technical assessment:
- Clear orchestration flow via `main_full_batch_flow` and `main_cdc_flow`.
- Reasonable DAG decomposition: ingest, curated, master, dbt are separated.
- Dedicated DAG for dbt full-refresh enables safer operational procedures.

Operational assessment:
- `max_active_runs=1` on main DAGs prevents race conditions.
- `wait_for_completion=True` trigger chain is simple and observable but increases end-to-end latency.
- Some configs remain hard-coded (`cluster`, `project`, bucket), not yet optimized for multi-environment use.

Risk level: Medium  
Priority:
- Standardize config via env vars/Variables.
- Add SLA timeouts and task-level alert policies.

### 16.2 `pipelines/postgres_to_gcs.py` + `raw_to_staging.py` + `staging_to_silver.py` (Snapshot Flow)
Technical assessment:
- Standard 3-tier ETL flow, well-structured with history artifacts.
- `raw_to_staging` includes quality checks and quarantine.
- No explicit table-level parallelism in snapshot extract/load.

Operational assessment:
- Easy to replay by `LOAD_DATE`/`RUN_ID`.
- Runtime may grow as volume increases due to sequential processing.
- Small-file accumulation and memory pressure need attention for large chunks.

Risk level: Medium  
Priority:
- Parallelize by table and tune adaptive chunking.
- Add fixed benchmarks by table class (small/medium/large).

### 16.3 `pipelines/cdc/normalize/*` (CDC Normalize)
Technical assessment:
- Contract-driven normalization, complete CDC metadata.
- Well-modularized (`cdc_normalizer`, `schema_applier`, `table_processor`, `io_utils`).
- Manifest output enables good run traceability.

Operational assessment:
- High reliability for rerun by `source_date_local`/`run_id`.
- Reduce unnecessary actions (count/debug) to stabilize Dataproc cost.
- Tightly coupled to quality of input AVRO metadata.

Risk level: Medium  
Priority:
- Default debug metrics to OFF.
- Optimize file sizing/coalesce strategy by volume.

### 16.4 `pipelines/cdc/merge/run_query_merge_v2.updated3.sh` (BigQuery Merge)
Technical assessment:
- Dual-branch watermark (`real lsn` + `inferred`), good idempotency.
- Optimized with run-scoped source + dedupe temp table before MERGE.
- Dynamic SQL is complex but has contract validation and identifier safety.

Operational assessment:
- Most critical module for BigQuery cost (slot-ms/bytes).
- `ONLY_TABLE` mode is very useful for incident reruns.
- Schema/partition governance for stage-silver is needed to sustain performance long-term.

Risk level: Medium-High  
Priority:
- Monitor bytes processed per table daily.
- Add regression checks for watermark monotonicity.

### 16.5 `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`
Technical assessment:
- Clear rule engine: null policy + DQ checks + quarantine + metrics.
- Adaptive partitions and AQE configs already applied.
- Rule alias resolution reduces contract/rules mismatch.

Operational assessment:
- Easy to audit via `dq_run_metrics` and `dq_reason`.
- Scalability depends on per-table rules JSON design.
- Rules versioning should be managed strictly (change log + approval flow).

Risk level: Low-Medium  
Priority:
- Set alert thresholds by bad ratio per table.
- Split heavy tables into independent jobs when runtime grows.

### 16.6 `pipelines/gen_master/*` (Master Generation)
Technical assessment:
- `gen_master_drug_code_medicines_script_v2.py`: driver-wide `toPandas` removed, using `mapInPandas`.
- `gen_master_patient_id_script_patched.py`: runtime still uses Python UDF path (not yet fully on SQL-expression path).
- Business logic is dense; stronger modularization needed for testability.

Operational assessment:
- Drug module scalability significantly improved.
- Patient module has CPU overhead risk as data grows.
- Two master tasks running in parallel may contend for resources on small clusters.

Risk level:
- Drug: Low-Medium
- Patient: Medium-High

Priority:
- Migrate patient ID to Spark native expression path.
- Set separate resource profiles per master job.

### 16.7 `dbt/models/platinum/*`
Technical assessment:
- Clear dim/fact layering, `valid/invalid` pattern is good for data quality.
- Many large facts migrated to incremental+merge + partition/cluster.
- Partition count limit resolved by setting `interval=100` for int range.

Operational assessment:
- Build time visibly improved after incremental migration.
- Full-refresh bootstrap is required when changing materialization.
- Some models still depend on legacy/rev branch logic; governance needed over time.

Risk level: Medium  
Priority:
- Standardize incremental windows by domain.
- Establish periodic full-refresh cadence for high late-arriving models.

### 16.8 `dbt/models/gold/*`
Technical assessment:
- Gold marts cover key KPIs (operational, inventory, disease, revenue).
- Most time-series models migrated to incremental.
- Keeping `gold_clinic_patient_visits_monthly` as table is the correct correctness decision.

Operational assessment:
- Suitable for daily batch BI serving.
- Business logic drift (not just schema tests) needs monitoring.
- Contract tests for critical KPIs (retention, cancellation rate, revenue) should be added.

Risk level: Medium  
Priority:
- Add business assertions by domain.
- Track freshness end-to-end from silver to gold.

### 16.9 `monitoring/*`
Technical assessment:
- Comprehensive SQL monitor set: watermark, freshness, duplicates, job health.
- Runbook and dashboard templates included.
- Exporter to custom metrics available for Cloud Monitoring integration.

Operational assessment:
- High operational readiness if Scheduled Queries and Log Sinks are configured correctly.
- Monitor quality depends on complete log export data.
- Clear ownership needed for alert triage and on-call actions.

Risk level: Low-Medium  
Priority:
- Standardize output table naming and retention policy.
- Conduct periodic alert fire drills to validate runbook.

### 16.10 `streaming_alert/*` (Real-time Alert)
Technical assessment:
- Reasonable event-driven architecture: Debezium -> Dataflow -> Pub/Sub -> Cloud Run.
- Includes deduplication + cooldown + audit history in BigQuery.
- Suitable for near real-time alerting use cases.

Operational assessment:
- Many moving parts (Pub/Sub, Run, Dataflow, Firestore) require strong SRE discipline.
- Higher risk of IAM/secret/runtime dependency issues compared to batch pipelines.
- DLQ, retry, and push endpoint auth need regular monitoring.

Risk level: Medium-High  
Priority:
- Set dedicated SLOs for alert latency and delivery success.
- Add clear dead-letter strategy and replay procedures.

### 16.11 `contract/*` (Data Contracts)
Technical assessment:
- Backbone for normalize/merge/curated.
- JSON validation and identifier safety enforced in the merge script.

Operational assessment:
- Contract changes are high-impact; require a review/approval release process.
- Version history and run-to-contract mapping must be stored for auditability.

Risk level: Medium  
Priority:
- Enforce contract CI checks before deployment.
- Store contract version metadata in run artifacts.

### 16.12 `manual_file/` (Cloud Run Manual File ETL)
Technical assessment:
- Clean event-driven architecture: GCS event -> Pub/Sub -> Cloud Run, no Airflow needed.
- Subfolder-based routing is flexible — adding a new file type only requires an INSERT into `__pipeline_config`, no redeployment.
- Staging uses `WRITE_TRUNCATE` — staging table always reflects the latest upload, suitable for reference data.
- IDs use `ABS(FARM_FINGERPRINT(...))` to ensure idempotency on re-upload of the same data.

Operational assessment:
- Pub/Sub may delay 10-15 minutes when `amaz-raw` bucket receives heavy CDC events simultaneously — monitor if upload SLA is strict.
- Staging path is fixed (no timestamp), always overwritten — no staging history retained, only raw history on GCS.
- No clear retry strategy if a pipeline step fails mid-way.

Risk level: Low-Medium  
Priority:
- Add alerting when Cloud Run returns 5xx errors.
- Consider adding idempotency guard for `public_users` to prevent duplicates on re-upload.
- Monitor Pub/Sub subscription lag as CDC volume grows.

---

## 17) Suggested Operating Model for Data Engineering Team

### Ownership Split
- Platform/DE: DAG + Spark + merge runtime + cost/performance.
- Analytics Engineering: dbt models/tests + semantic correctness.
- Data Quality/Ops: monitors, alert routing, incident playbook.

### Weekly Review Pack (Minimum)
- DAG p50/p95 duration.
- BigQuery bytes processed + slot-ms by module.
- Quarantine ratio and duplicate PK trend.
- Freshness lag: silver/platinum/gold.
- Top 5 costliest jobs + optimization action items.
