# Metrics Guide (GCP Monitoring)

This document describes the metrics currently used in the project, their meaning, and how to read them for production operations.

## 1) Monitoring Flow

- Cloud Logging sink -> `ops_logs.*`
- Scheduled Query -> `ops_monitor.*`
- Exporter (`monitoring/exporter/ops_monitor_to_custom_metrics.py`) -> `custom.googleapis.com/ops/*`
- Cloud Monitoring Dashboard -> reads custom metrics and system metrics

## 2) Custom Metrics (Primary Source for Ops Dashboard)

Resource type for custom metrics: `global`  
Required resource label: `project_id`

| Metric | Source (ops_monitor) | Label | Meaning | How to Read |
|---|---|---|---|---|
| `custom.googleapis.com/ops/watermark/mins_since_update` | `daily_watermark_health` | `table_name` | Minutes since the last watermark update | Continuously increasing -> pipeline may be stalled or delayed |
| `custom.googleapis.com/ops/watermark/mins_since_last_commit` | `daily_watermark_health` | `table_name` | Lag relative to the latest source commit | Higher than baseline -> CDC is lagging |
| `custom.googleapis.com/ops/bq/jobs_total` | `hourly_bq_job_health` | `scope=all` | Total BQ jobs in the hour | Sudden drop without traffic decrease -> possible scheduler failure |
| `custom.googleapis.com/ops/bq/jobs_failed` | `hourly_bq_job_health` | `scope=all` | Number of failed jobs | Sudden spike -> check query/job errors |
| `custom.googleapis.com/ops/bq/failed_ratio` | `hourly_bq_job_health` | `scope=all` | Failure ratio over total jobs | Preferred for alerting (more stable than absolute count) |
| `custom.googleapis.com/ops/bq/bytes_processed` | `hourly_bq_job_health` | `scope=all` | Data scanned by queries | Sudden spike -> risk of cost overrun |
| `custom.googleapis.com/ops/bq/slot_ms` | `hourly_bq_job_health` | `scope=all` | Slot consumption | Compare against quota/slot commitment |
| `custom.googleapis.com/ops/bq/p95_duration_sec` | `hourly_bq_job_health` | `scope=all` | p95 query duration | Steady increase -> resource contention or SQL regression |
| `custom.googleapis.com/ops/composer/total_events` | `hourly_composer_airflow_health` | `component` | Total log events by component | Abnormal event drop -> possible log/sink issue |
| `custom.googleapis.com/ops/composer/error_events` | `hourly_composer_airflow_health` | `component` | Error-severity events | Track which component is generating errors (scheduler/worker/etc.) |
| `custom.googleapis.com/ops/composer/error_hints_text` | `hourly_composer_airflow_health` | `component` | Logs containing error keywords | Use as early-warning heuristic |
| `custom.googleapis.com/ops/datastream/total_events` | `hourly_datastream_health` | `stream_id` | Total stream events | Unusual drop -> stream may have an issue |
| `custom.googleapis.com/ops/datastream/error_events` | `hourly_datastream_health` | `stream_id` | Error-severity stream events | Primary incident metric for Datastream |
| `custom.googleapis.com/ops/datastream/lag_or_error_hints` | `hourly_datastream_health` | `stream_id` | Logs with lag/backlog/error hints | Rises before stream failure becomes explicit |
| `custom.googleapis.com/ops/gcs/objects_created` | `hourly_gcs_ingestion` | `bucket_name`, `table_id` | New objects written to GCS | Use to verify ingestion is running |
| `custom.googleapis.com/ops/gcs/objects_deleted` | `hourly_gcs_ingestion` | `bucket_name`, `table_id` | Objects deleted | Sudden spike -> review retention/purge policy |
| `custom.googleapis.com/ops/gcs/error_events` | `hourly_gcs_ingestion` | `bucket_name`, `table_id` | GCS ingest-related error events | High value -> check IAM/path/pipeline |
| `custom.googleapis.com/ops/scheduler/total_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Total Scheduler events | Baseline activity reference |
| `custom.googleapis.com/ops/scheduler/error_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Error-severity events | Use for scheduler failure alerts |
| `custom.googleapis.com/ops/scheduler/http_4xx_5xx_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Jobs returning HTTP error >= 400 | Distinguish app errors from permission errors |
| `custom.googleapis.com/ops/scheduler/http_5xx_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Server errors from target | Should alert quickly |
| `custom.googleapis.com/ops/scheduler/success_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Successful executions | Combine with total to calculate health ratio |
| `custom.googleapis.com/ops/scheduler/non_success_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Non-successful executions | Use for error trend tracking |
| `custom.googleapis.com/ops/scheduler/success_ratio` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Success rate | Primary SLA metric for the scheduler |
| `custom.googleapis.com/ops/dag/critical_p95_minutes` | `daily_critical_dag_slo` | `dag_id` | p95 runtime of critical DAGs | Compare against restore window |
| `custom.googleapis.com/ops/dag/recovery_time_minutes` | `daily_critical_dag_slo` | `dag_id` | p95 time from failure to next successful run | High value -> large MTTR, optimize retry/runbook |
| `custom.googleapis.com/ops/dag/restore_buffer_minutes` | `daily_critical_dag_slo` | `dag_id` | `restore_window - critical_p95` | Negative value means recovery time budget is exceeded |
| `custom.googleapis.com/ops/dag/restore_window_minutes` | `daily_critical_dag_slo` | `dag_id` | Allowed recovery window per policy | Should be a fixed reference value |

## 3) System Metrics Used in Dashboard JSONs

### BigQuery Dashboard (`monitoring/dashboard/BigQuery.json`)

- `bigquery.googleapis.com/query/count`
- `bigquery.googleapis.com/query/execution_times`
- `bigquery.googleapis.com/slots/allocated_for_project`
- `bigquery.googleapis.com/slots/assigned`
- `bigquery.googleapis.com/slots/max_assigned`
- `bigquery.googleapis.com/storage/table_count`
- `bigquery.googleapis.com/storage/stored_bytes`
- `bigquery.googleapis.com/storage/uploaded_row_count`

Quick reading guide:
- Query count + execution time rising together -> genuine load increase.
- Assigned slots near max -> risk of queue/slowdown.
- Stored bytes/uploaded rows spike -> check for unusual data or cost risk.

### Cloud Run Dashboard (`monitoring/dashboard/Cloud Run Monitoring.json`)

- `run.googleapis.com/request_count`
- `run.googleapis.com/request_latencies`
- `run.googleapis.com/container/instance_count`
- `run.googleapis.com/container/billable_instance_time`
- `run.googleapis.com/container/cpu/allocation_time`
- `run.googleapis.com/container/memory/allocation_time`
- `run.googleapis.com/container/cpu/utilizations`
- `run.googleapis.com/container/memory/utilizations`

Quick reading guide:
- p95 latency rising but request count flat -> app or dependency issue.
- 5xx / request_count rising -> prioritize as incident.
- High CPU/Mem utilization sustained + slow instance count growth -> needs scale tuning.

### Composer Dashboard (`monitoring/dashboard/Composer Ops.json`)

- `composer.googleapis.com/environment/dag_processing/total_parse_time`
- `composer.googleapis.com/environment/executor/queued_tasks`
- `composer.googleapis.com/environment/executor/running_tasks`
- `composer.googleapis.com/environment/scheduler/pod_eviction_count`
- `composer.googleapis.com/environment/worker/pod_eviction_count`
- `composer.googleapis.com/workload/cpu/usage_time`
- `composer.googleapis.com/workload/memory/bytes_used`
- `composer.googleapis.com/workflow/task/run_duration`

Quick reading guide:
- `queued_tasks` rising, `running_tasks` flat -> executor bottleneck.
- `pod_eviction_count > 0` repeatedly -> resource pressure or K8s issue.
- Task `run_duration` steadily increasing -> DAG/dependency regression.

### Dataproc Dashboard (`monitoring/dashboard/Dataproc Cluster [cluster-8649].json`)

- `dataproc.googleapis.com/cluster/yarn/memory_size`
- `dataproc.googleapis.com/cluster/yarn/pending_memory_size`
- `dataproc.googleapis.com/cluster/yarn/nodemanagers`
- `dataproc.googleapis.com/cluster/hdfs/storage_capacity`
- `compute.googleapis.com/instance/cpu/utilization`
- Network bytes/packets in/out
- Disk bytes/ops read/write

Quick reading guide:
- `pending_memory_size` rising + `nodemanagers` not growing -> queue pressure.
- High CPU + sustained high disk/network -> cluster near capacity, consider autoscaling.

### Datastream Dashboard (`monitoring/dashboard/DS latency.json`)

- `datastream.googleapis.com/stream/latencies`
- `datastream.googleapis.com/stream/freshness`

Quick reading guide:
- Latency/freshness continuously rising -> stream backlog.
- Cross-reference with custom `ops/datastream/error_events` to distinguish failure from mere delay.

## 4) How to Read Metrics During an Incident

- Step 1: Identify the scope (source ingest, orchestration, transform, serving).
- Step 2: Check ratio metrics first (`failed_ratio`, `success_ratio`), then absolute counts.
- Step 3: Use p95/p99 for SLO decisions — never use averages.
- Step 4: If a custom metric unexpectedly shows 0, immediately check:
  - Whether the scheduled query has recent data in `ops_monitor.*`
  - Whether the exporter job is successfully pushing data points
  - Whether the dashboard `project_id` filter is set correctly

## 5) Suggested Alert Thresholds (Initial Reference)

- Watermark stale > 180 minutes
- BQ `failed_ratio` > 0.05 for 15 minutes
- Scheduler `success_ratio` < 0.95 for 30 minutes
- Datastream `error_events` > 0 for 15 consecutive minutes
- DAG `restore_buffer_minutes < 0` (critical)
- DAG `recovery_time_minutes > restore_window_minutes` (critical)

Calibrate against 2–4 weeks of real operational baseline before finalizing thresholds.
