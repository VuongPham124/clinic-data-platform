# Performance Optimization Strategy

## Objective
This document provides a detailed performance optimization strategy for the current workspace data flow:
- Snapshot ingest (`postgres_to_gcs.py` -> `raw_to_staging.py` -> `staging_to_silver.py`)
- CDC ingest and merge (`pipelines/cdc/normalize/*` + `run_query_merge_v2.updated3.sh`)
- Curated quality layer (`silver_to_silver_curated_dq_v2_3.py`)
- Master generation (`pipelines/gen_master/*`)
- dbt transformations (`dbt/models/*`)
- Orchestration (`dags/*`)

The strategy is organized by module with:
1. Current bottlenecks
2. Optimization proposals
3. Implementation approach
4. Measurable acceptance criteria

## 1) End-to-End Baseline and SLO
Before changing code, capture baseline for 7 days:
- DAG runtime p50/p95 for:
  - `full_snapshot_batch_pipeline`
  - `cdc_daily_to_silver`
  - `silver_to_silver_curated_dq_v2_3`
  - `dbt_platinum_gold_build_test`
- Dataproc job durations and shuffle metrics
- BigQuery bytes processed and slot-ms for merge/dbt
- Data freshness lag (`monitoring/sql/03_silver_freshness.sql`)

Target SLO (recommended initial):
- CDC daily flow p95 < 90 minutes
- Full batch flow p95 < 4 hours
- Silver freshness lag < 120 minutes
- BigQuery failed ratio < 5% per hour

## 2) Snapshot Flow

## 2.1 `pipelines/postgres_to_gcs.py`
Current bottlenecks:
- Tables processed sequentially; no table-level parallelism.
- In-memory gzip buffer (`BytesIO`) per chunk can increase memory pressure on large chunks.
- Chunking by numeric `id` may produce sparse scans if IDs are fragmented.

Optimization proposals:
- Add table-level parallel execution (small worker pool) for independent tables.
- Replace full in-memory buffer with temp-file based streaming upload.
- Adaptive chunk sizing by table cardinality (large tables smaller chunks, small tables larger chunks).
- Move table list into config/contract so low-value tables can be excluded in snapshot runs.

Implementation approach:
- Introduce `MAX_TABLE_WORKERS` env var (start 2-4).
- Use `tempfile.NamedTemporaryFile()` + `upload_from_filename`.
- Pre-query per-table row count and choose chunk size tiers.

Acceptance criteria:
- Snapshot extract wall-clock reduced by >= 30% on staging benchmark.
- Peak memory usage on worker reduced by >= 40% for largest tables.
- No increase in extraction error rate.

## 2.2 `pipelines/raw_to_staging.py`
Current bottlenecks:
- Per-table `ok_df.count()` and `bad_df.count()` trigger extra full scans.
- `mode("overwrite")` writes entire table partitions for each run.
- CSV parsing options (`multiLine=true`) may be expensive for all tables.

Optimization proposals:
- Replace separate `count()` actions with one-pass aggregation (single job).
- Persist intermediate DataFrame only when reused multiple times.
- Tune Spark read/write settings per table size (small vs large tables).
- Write partitioned parquet by logical partition (`load_date`, `run_id`) consistently.

Implementation approach:
- Use `groupBy`/`agg` on validity flag to collect both counts in one action.
- Set adaptive query execution and shuffle partitions dynamically.
- Add optional table-level parallelization in this stage as separate Spark jobs for heavy tables.

Acceptance criteria:
- Reduce Dataproc job duration for raw->staging by >= 20%.
- Reduce Spark total task time and shuffle read by >= 25% on large tables.
- Output row counts unchanged vs baseline.

## 2.3 `pipelines/staging_to_silver.py`
Current bottlenecks:
- `WRITE_TRUNCATE` reloads whole table every run.
- One-table-at-a-time load jobs (serial) increases total runtime.

Optimization proposals:
- Move to partition-aware incremental loads when possible.
- Parallelize BigQuery load jobs with controlled concurrency.
- Pre-create partitioned/clustered silver tables for query and merge efficiency.

Implementation approach:
- For snapshot runs, keep truncate for full reset scenarios only.
- Add mode switch: `FULL_REPLACE` vs `INCREMENTAL_APPEND/MERGE`.
- Submit load jobs concurrently (e.g., 4-8 concurrent jobs with retry/backoff).

Acceptance criteria:
- Stage->silver runtime reduced by >= 35%.
- BigQuery bytes processed for downstream workloads reduced due to partition/cluster pruning.

## 3) CDC Normalize and Merge

## 3.1 `pipelines/cdc/normalize/table_processor.py`
Current bottlenecks:
- Debug `count()` on `__lsn_num` null checks causes extra full scans.
- Separate `ok_df.count()` and `bad_df.count()` actions.
- `coalesce(args.ok_files)` may underutilize cluster for large tables.

Optimization proposals:
- Disable expensive debug counts by default (`DEBUG_METRICS=false`).
- Replace multi-action counting with single aggregated metrics collection.
- Use size-aware file count strategy instead of fixed `ok_files`/`bad_files`.

Implementation approach:
- Add runtime flag to guard debug actions.
- Derive target output file count from estimated input size.
- Keep coalesce only for small tables; use repartition for large tables.

Acceptance criteria:
- CDC normalize stage reduced by >= 15-25% runtime.
- Output file size distribution improves (fewer tiny files).

## 3.2 `pipelines/cdc/normalize/cdc_normalizer.py`
Current bottlenecks:
- Heavy string parsing and regex transformations on every record.
- LSN fallback logic is robust but compute intensive.

Optimization proposals:
- Apply expensive transformations only when required columns exist.
- Cache compiled expressions where possible and avoid repeated casts.
- For very large tables, consider split path for records with valid LSN vs inferred LSN.

Implementation approach:
- Add fast-path branch: valid LSN records bypass fallback extraction logic.
- Benchmark CPU-heavy expressions using Spark UI stages.

Acceptance criteria:
- CPU time per 1M records reduced by >= 15% on CDC heavy tables.

## 3.3 `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`
Current bottlenecks:
- Dynamic SQL may scan large stage tables when filters are not fully pruned.
- Multiple `ROW_NUMBER()` windows and unions can be expensive.
- Casting in SELECT during MERGE increases slot usage.

Optimization proposals:
- Partition and cluster `bq_stage.*_cdc` and `silver.*` by commit date + PK/hash keys.
- Restrict MERGE source to run/date window explicitly before windowing.
- Materialize pre-filtered source temp table per run, then MERGE from that compact table.
- Use `ONLY_TABLE` mode routinely for targeted reruns.

Implementation approach:
- Add table DDL management to ensure partition/cluster once.
- Create `run_scoped_source` temp table with strict run/date predicates.
- Benchmark bytes processed before/after each SQL rewrite.

Acceptance criteria:
- BigQuery `slot_ms` for merge reduced by >= 30%.
- Bytes processed per CDC run reduced by >= 40% for large entities.
- Watermark correctness unchanged.

## 4) Curated DQ Layer

## `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`
Current bottlenecks:
- `persist + repartition(16/8)` is static and may be suboptimal across table sizes.
- Good/bad metrics currently rely on union/count collection; still extra compute overhead.
- Writing quarantine with schema alignment each run may trigger additional reads.

Optimization proposals:
- Make repartition adaptive by row count or input size.
- Reduce repeated read/write of quarantine schema by caching target schema map.
- Process selected large tables in separate jobs (horizontal DAG split).

Implementation approach:
- Add per-table config for partition count and DQ strictness.
- Introduce broadcast hints for small dimension-like lookups if any join is added.
- Enable AQE + skew handling Spark configs explicitly.

Acceptance criteria:
- Curated DAG runtime reduced by >= 20%.
- No regression in DQ bad-row detection.

## 5) Master Data Generation

## 5.1 `gen_master_drug_code_medicines_script_v2.py` and `..._patched.py`
Current bottlenecks:
- Uses `toPandas()` after Spark read; this is driver-memory bound and non-scalable.
- Includes full DataFrame `count()` before conversion.

Optimization proposals:
- Rewrite transformation logic in Spark-native operations (no Pandas on driver).
- If full rewrite is not immediate, process in batches/chunks by deterministic key ranges.
- Keep strict maximum row guard until Spark rewrite is complete.

Implementation approach:
- Phase 1: Introduce chunked processing mode by source ID ranges.
- Phase 2: Convert regex and normalization rules to Spark UDF or SQL expressions.
- Phase 3: remove `toPandas()` path from production DAG.

Acceptance criteria:
- Supports >= 5x current row volume without driver OOM risk.
- Runtime variability reduced (stable p95).

## 5.2 `gen_master_patient_id_script_patched.py`
Current bottlenecks:
- UDF-heavy string normalization can be CPU-expensive.

Optimization proposals:
- Replace Python UDF with Spark SQL native functions where possible.
- Pre-trim and normalize only required columns.

Acceptance criteria:
- Task CPU time reduced >= 20% on same data volume.

## 6) dbt Layer (`dbt/models/platinum`, `dbt/models/gold`)
Current bottlenecks:
- Most models materialized as `table`; full rebuild is expensive.
- Potential re-scan of large fact sources for daily refresh.

Optimization proposals:
- Convert high-volume facts to `incremental` with partition + cluster strategy.
- Add `is_incremental()` filters based on `__commit_ts` or date keys.
- Keep small dimensions as table/view depending on access pattern.
- Add model-level performance tests (bytes processed and runtime budget).

Implementation approach:
- Prioritize incremental migration for:
  - `fact_operational_clinic_bookings`
  - `fact_inventory_export`
  - `fact_inventory_import`
  - `fact_inventory_snapshot`
  - `fact_prescription`
- Add post-run metadata capture from `INFORMATION_SCHEMA.JOBS`.

Acceptance criteria:
- dbt platinum build runtime reduced by >= 40%.
- Gold build runtime reduced by >= 25%.
- Data correctness parity with baseline outputs.

## 7) Orchestration and Scheduling (`dags/*`)
Current bottlenecks:
- Main DAGs are strictly sequential (`wait_for_completion=True` chain).
- Some heavy tasks could run concurrently without data dependency.

Optimization proposals:
- Split independent branches (example: master generation + selective dbt subsets when safe).
- Tune retries/timeouts and add SLA-based alerts for slow tasks.
- Introduce workload-aware scheduling windows to reduce slot contention.

Implementation approach:
- Build dependency matrix by dataset impact.
- Move non-blocking tasks to parallel branches gradually.
- Add max_active_runs and pools to prevent noisy-neighbor effects.

Acceptance criteria:
- End-to-end flow p95 reduced by >= 20% without increased failure rate.

## 8) Platform-Level Tuning
Dataproc:
- Enable autoscaling policy with min/max workers tuned by stage.
- Tune `spark.sql.shuffle.partitions`, AQE, skew join handling.
- Use larger executors only for shuffle-heavy stages, not globally.

BigQuery:
- Partition by date key / commit date, cluster by PK and join keys.
- Monitor slot usage and consider reservations if sustained load exists.
- Enforce query cost guardrails for non-critical jobs.

Storage layout:
- Control output file size target (128-512MB parquet files).
- Eliminate small-file proliferation in staging/quarantine paths.

Acceptance criteria:
- Lower p95 runtime and lower cost per successful pipeline run.

## 9) KPI Dashboard for Optimization Program
Track weekly:
- Runtime: DAG/task p50/p95
- Throughput: rows/minute by stage
- Cost: BQ bytes processed, slot-ms, Dataproc VM-hours
- Reliability: failure ratio, retry ratio
- Data timeliness: freshness lag, watermark lag
- Quality stability: quarantine ratio

Use existing monitors in `monitoring/sql/` as source of truth.

## 10) Prioritized Roadmap

Phase A (1-2 weeks, low risk, high impact):
1. Remove/guard expensive debug counts in CDC normalize.
2. Reduce duplicate actions in Spark stages (single-pass metrics).
3. Add controlled parallelism for BQ load jobs in `staging_to_silver.py`.
4. Add merge-source pruning improvements in merge script.

Phase B (2-4 weeks):
1. Adaptive repartition/coalesce strategy in CDC + curated jobs.
2. Partition/cluster hardening for stage/silver/platinum tables.
3. DAG branch parallelization where dependencies allow.

Phase C (4-8 weeks):
1. Rewrite master drug pipeline to Spark-native (remove `toPandas`).
2. Convert large dbt fact models to incremental.
3. Implement continuous performance regression tests in CI/CD.

## 11) Performance Test Protocol (for every optimization PR)
Required evidence:
- Before/after runtime for same input window
- Before/after BigQuery bytes processed and slot-ms
- Before/after Spark stage metrics (task time, shuffle read/write)
- Data parity checks (row counts, PK duplicates, key aggregates)

A change is accepted only if:
- Runtime or cost improves materially
- Data parity checks pass
- Operational reliability is unchanged or improved
