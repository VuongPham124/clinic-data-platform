# Data Platform Risk & Operations Guide

Updated: 2026-03-06  
System scope: snapshot + CDC + curated + master + dbt + monitoring.

## 1) System map (for incident context)

Primary flows:
- Full batch:
  - `main_full_batch_flow` -> `full_snapshot_batch_pipeline` -> `silver_to_silver_curated_dq_v2_3` -> `gen_master_parallel` -> `dbt_platinum_gold_build_test`
- CDC daily:
  - `main_cdc_flow` -> `cdc_daily_to_silver` -> `silver_to_silver_curated_dq_v2_3` -> `gen_master_parallel` -> `dbt_platinum_gold_build_test`

Critical modules:
- Snapshot ingest: `pipelines/postgres_to_gcs.py`, `pipelines/raw_to_staging.py`, `pipelines/staging_to_silver.py`
- CDC merge: `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`
- Curated DQ: `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`
- Master: `pipelines/gen_master/*`
- dbt: `dbt/models/platinum/*`, `dbt/models/gold/*`
- Monitoring SQL: `monitoring/sql/*`

---

## 2) Source outage thi sao?

### Typical outage cases
- PostgreSQL source unavailable / high latency.
- Datastream/CDC source lagging or disconnected.
- GCS ingest bucket permission/network outage.

### Symptoms
- Airflow tasks fail at extract/normalize/load steps.
- Watermark stale in `silver_meta.watermarks`.
- Freshness lag spikes in `monitoring/sql/03_silver_freshness.sql`.

### Immediate response (0-30 minutes)
1. Freeze non-essential downstream runs:
   - Pause manual triggers for `main_full_batch_flow` and `main_cdc_flow`.
2. Confirm outage scope:
   - Single source table vs whole source system.
3. Validate infra health:
   - Dataproc cluster workers, Composer worker logs, BigQuery job health.

### Recovery strategy
- If source recovers quickly:
  - Re-run failed DAG with same `run_id` where possible (idempotent paths first).
- If outage is prolonged:
  - Switch to degraded mode:
    - run only critical tables using `ONLY_TABLE` in CDC merge.
    - skip non-critical marts in dbt selection until source stable.

### Preventive controls
- Define source SLA and alert on stale watermark/freshness.
- Keep per-domain critical table list for degraded execution mode.
- Maintain runbook commands for rapid rerun (by date/run_id/table).

---

## 3) Schema change thi sao?

### Typical schema changes
- New column added in source.
- Column type changed (string -> numeric, timestamp format change).
- Column dropped/renamed.

### Where impact appears
- CDC normalize cast (`schema_applier.py`).
- Merge SQL dynamic column alignment in `run_query_merge_v2.updated3.sh`.
- dbt source contracts (`src_silver.yml`, `src_platinum.yml`) and downstream models.

### Response model
1. Detect:
   - Contract validation fails or cast failures spike.
2. Classify change:
   - backward-compatible (add column) vs breaking (drop/rename/type break).
3. Apply contract-first patch:
   - update `contract/*.json`.
   - update dbt source + affected model SQL.
4. Validate in staging:
   - run single-table path first.
5. Promote with rollback path:
   - keep previous contract version and pin if needed.

### Strong recommendation
- Enforce pre-deploy checks:
  - contract JSON validation
  - dbt compile/build in staging
  - one-table canary merge

---

## 4) Corrupted data thi sao?

### Corruption patterns
- Malformed timestamp strings.
- Invalid numeric payloads.
- PK null/duplicate anomalies.
- CDC metadata missing (`__lsn_num`, `__commit_ts`, `__run_id`).

### Existing controls in your system
- Quarantine split in raw/staging/curated.
- Valid/invalid model split in platinum facts.
- DQ metrics table: `silver_curated_dq.dq_run_metrics`.
- Monitoring SQL for quality/freshness/duplicates.

### Incident workflow
1. Contain:
   - isolate impacted table(s) and stop broad DAG reruns.
2. Quantify:
   - compute bad ratio by table/run_id.
3. Root cause:
   - source defect vs parser/cast defect vs contract drift.
4. Remediate:
   - fix parser/contract/rule.
   - replay affected date range by table.
5. Verify:
   - compare row counts, PK duplicates, business aggregates pre/post fix.

### Operational policy
- Never silently drop corrupted records.
- Always route to quarantine/invalid with reason.
- Keep audit trail by run_id and source_date_local.

---

## 5) Backfill the nao?

## 5.1 Snapshot backfill
Use when:
- historical partitions missing/corrupted.

Recommended steps:
1. Choose backfill window (`LOAD_DATE`, `RUN_ID` per batch).
2. Run:
   - `postgres_to_gcs.py` -> `raw_to_staging.py` -> `staging_to_silver.py`
3. Rebuild curated/master/dbt for same window.
4. Reconcile row count and key aggregates.

Notes:
- Prefer isolated `RUN_ID` naming for backfill (`backfill_YYYYMMDD_*`).
- Avoid running large backfill together with peak CDC windows.

## 5.2 CDC backfill
Use when:
- missed CDC days / merge failure window.

Recommended steps:
1. Normalize CDC for target dates.
2. Merge by table using `ONLY_TABLE` when possible.
3. Verify watermark monotonicity and freshness recovery.

Notes:
- Use run-scoped merge behavior to reduce full-stage scans.
- Replay in chronological order to avoid semantic drift.

## 5.3 dbt backfill
Use when:
- model logic changed, materialization changed, or data correction applied.

Recommended steps:
1. For structural changes: run full-refresh DAG:
   - `dbt_platinum_gold_full_refresh`
2. For bounded fixes:
   - run targeted selectors by model/domain.
3. Validate:
   - tests + metric parity on sampled windows.

---

## 6) Trade-off trong flow

### Snapshot vs CDC
- Snapshot:
  - Pros: deterministic rebuild, easier historical recovery.
  - Cons: heavier cost/runtime.
- CDC:
  - Pros: fresher, lower steady-state cost.
  - Cons: operational complexity (watermark/idempotency/schema drift).

### Curated DQ strictness
- Strict rules:
  - Pros: cleaner downstream analytics.
  - Cons: higher quarantine ratio, potential freshness delay.
- Relaxed rules:
  - Pros: better throughput.
  - Cons: higher data quality debt in marts.

### Incremental dbt vs full rebuild
- Incremental:
  - Pros: faster and cheaper daily.
  - Cons: needs careful late-arriving handling and periodic full refresh.
- Full rebuild:
  - Pros: simplest correctness.
  - Cons: expensive and slow at scale.

### Parallelism vs stability
- Higher parallelism:
  - Pros: shorter wall-clock.
  - Cons: resource contention on small clusters.
- Controlled concurrency:
  - Pros: predictable SLA.
  - Cons: longer total duration.

---

## 7) Cong nghe co phu hop voi luong du lieu khong?

Assessment by scale tier:

### Small scale (<= 5M rows/day, <= 20 GB/day)
- Current stack is sufficient but somewhat over-provisioned in complexity.
- Can reduce operational burden by simplifying some optional modules.

### Medium scale (5M-100M rows/day, 20 GB-1 TB/day)
- Current architecture is a good fit:
  - BigQuery for warehouse and merge.
  - Spark for normalize/curated/master transforms.
  - dbt incremental for marts.
- Key success factor: partition/cluster strategy and cluster sizing discipline.

### Large scale (> 100M rows/day, > 1 TB/day)
- Architecture still viable, but must harden:
  - stronger autoscaling + pool governance,
  - table-level execution isolation,
  - stricter cost/perf regression testing.
- Without this hardening, contention and cost volatility will dominate.

### Current bottleneck risk in your setup
- Dataproc cluster is small relative to parallel jobs.
- Some modules still carry UDF-heavy or legacy logic.
- Mixed materialization patterns require strict runbook discipline.

---

## 8) Practical runbook matrix (incident -> action)

- Source down:
  - pause triggers, run critical-table mode, resume with rerun.
- Schema break:
  - contract patch first, canary table run, then full domain.
- Corrupted payload:
  - quarantine + root-cause + bounded replay.
- Missed windows:
  - date-scoped backfill + watermark verification.
- dbt structural change:
  - full-refresh DAG then incremental steady-state.

---

## 9) Recommended operating SLO targets

- CDC flow p95 < 90 minutes.
- Full snapshot flow p95 < 4 hours.
- Silver freshness lag < 120 minutes.
- BigQuery failed ratio < 5% per hour.
- Quarantine ratio stable and alert when > baseline threshold.

---

## 10) Next hardening actions (high ROI)

1. Clean and standardize `gen_master_patient_id_script_patched.py` runtime path (move to Spark-native expression, remove legacy dead blocks).
2. Add CI gate for:
   - contract validation,
   - dbt compile/build smoke in staging.
3. Add weekly cost/perf report:
   - slot-ms, bytes processed, DAG p95 by module.
4. Define severity levels and on-call ownership per module group.
