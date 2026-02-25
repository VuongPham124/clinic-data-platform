# Metrics Guide (GCP Monitoring)

Tai lieu nay mo ta cac metric dang duoc dung trong project, y nghia va cach doc de van hanh production.

## 1) Monitoring flow

- Cloud Logging sink -> `ops_logs.*`
- Scheduled Query -> `ops_monitor.*`
- Exporter (`monitoring/exporter/ops_monitor_to_custom_metrics.py`) -> `custom.googleapis.com/ops/*`
- Cloud Monitoring Dashboard -> doc metric custom va metric he thong

## 2) Custom metrics (nguon chinh cho Ops dashboard)

Resource type cua custom metrics: `global`  
Resource label bat buoc: `project_id`

| Metric | Nguon (ops_monitor) | Label | Y nghia | Cach doc |
|---|---|---|---|---|
| `custom.googleapis.com/ops/watermark/mins_since_update` | `daily_watermark_health` | `table_name` | So phut tu lan cap nhat watermark gan nhat | Tang lien tuc -> pipeline co the dung/tre |
| `custom.googleapis.com/ops/watermark/mins_since_last_commit` | `daily_watermark_health` | `table_name` | Do tre so voi commit gan nhat cua source | Cao hon baseline -> CDC dang bi lag |
| `custom.googleapis.com/ops/bq/jobs_total` | `hourly_bq_job_health` | `scope=all` | Tong so job BQ trong khung gio | Giam dot ngot + traffic khong giam -> co the fail scheduler |
| `custom.googleapis.com/ops/bq/jobs_failed` | `hourly_bq_job_health` | `scope=all` | So job fail | Tang dot bien -> check query/job error |
| `custom.googleapis.com/ops/bq/failed_ratio` | `hourly_bq_job_health` | `scope=all` | Ti le fail tren tong job | Dung cho alert chinh (on dinh hon so tuyet doi) |
| `custom.googleapis.com/ops/bq/bytes_processed` | `hourly_bq_job_health` | `scope=all` | Du lieu query da quet | Dot bien cao -> nguy co cost spike |
| `custom.googleapis.com/ops/bq/slot_ms` | `hourly_bq_job_health` | `scope=all` | Slot tieu thu | So sanh voi quota/slot commitment |
| `custom.googleapis.com/ops/bq/p95_duration_sec` | `hourly_bq_job_health` | `scope=all` | p95 thoi gian query | Tang deu -> nghen tai nguyen/SQL regress |
| `custom.googleapis.com/ops/composer/total_events` | `hourly_composer_airflow_health` | `component` | Tong su kien log theo component | Mat event bat thuong -> co the log/sink loi |
| `custom.googleapis.com/ops/composer/error_events` | `hourly_composer_airflow_health` | `component` | So event co severity loi | Theo doi component gay loi (scheduler/worker/...) |
| `custom.googleapis.com/ops/composer/error_hints_text` | `hourly_composer_airflow_health` | `component` | So log co tu khoa loi | Dung nhu heuristic canh bao som |
| `custom.googleapis.com/ops/datastream/total_events` | `hourly_datastream_health` | `stream_id` | Tong event stream | Giam bat thuong -> stream co van de |
| `custom.googleapis.com/ops/datastream/error_events` | `hourly_datastream_health` | `stream_id` | Event severity loi cua stream | Metric su co chinh cho Datastream |
| `custom.googleapis.com/ops/datastream/lag_or_error_hints` | `hourly_datastream_health` | `stream_id` | So log co hint lag/backlog/error | Tang truoc khi stream fail ro rang |
| `custom.googleapis.com/ops/gcs/objects_created` | `hourly_gcs_ingestion` | `bucket_name`, `table_id` | So object moi ghi vao GCS | Dung de check nhap lieu co chay |
| `custom.googleapis.com/ops/gcs/objects_deleted` | `hourly_gcs_ingestion` | `bucket_name`, `table_id` | So object bi xoa | Dot bien can soat retention/purge |
| `custom.googleapis.com/ops/gcs/error_events` | `hourly_gcs_ingestion` | `bucket_name`, `table_id` | So event loi lien quan GCS ingest | Tang cao -> check IAM/path/pipeline |
| `custom.googleapis.com/ops/scheduler/total_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Tong event Scheduler | Muc tham chieu hoat dong |
| `custom.googleapis.com/ops/scheduler/error_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | So event severity loi | Dung cho alert fail scheduler |
| `custom.googleapis.com/ops/scheduler/http_4xx_5xx_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Job tra ma loi HTTP >=400 | Phan tach loi app/permission |
| `custom.googleapis.com/ops/scheduler/http_5xx_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Loi server tu target | Nen alert nhanh |
| `custom.googleapis.com/ops/scheduler/success_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | So lan execute thanh cong | Ket hop voi total de tinh suc khoe |
| `custom.googleapis.com/ops/scheduler/non_success_events` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Khong thanh cong | Phuc vu trend error |
| `custom.googleapis.com/ops/scheduler/success_ratio` | `hourly_cloud_scheduler_health` | `job_id`, `scheduler_location` | Ti le thanh cong | Metric SLA chinh cua scheduler |
| `custom.googleapis.com/ops/dag/critical_p95_minutes` | `daily_critical_dag_slo` | `dag_id` | p95 run time cua critical DAG | So sanh voi restore window |
| `custom.googleapis.com/ops/dag/recovery_time_minutes` | `daily_critical_dag_slo` | `dag_id` | p95 thoi gian tu fail den run success ke tiep | Cao -> MTTR lon, can toi uu retry/runbook |
| `custom.googleapis.com/ops/dag/restore_buffer_minutes` | `daily_critical_dag_slo` | `dag_id` | `restore_window - critical_p95` | < 0 la vuot ngan sach thoi gian khoi phuc |
| `custom.googleapis.com/ops/dag/restore_window_minutes` | `daily_critical_dag_slo` | `dag_id` | Cua so khoi phuc cho phep theo policy | Nen la duong reference/co dinh |

## 3) System metrics dang duoc dung trong cac dashboard JSON

### BigQuery dashboard (`monitoring/dashboard/BigQuery.json`)

- `bigquery.googleapis.com/query/count`
- `bigquery.googleapis.com/query/execution_times`
- `bigquery.googleapis.com/slots/allocated_for_project`
- `bigquery.googleapis.com/slots/assigned`
- `bigquery.googleapis.com/slots/max_assigned`
- `bigquery.googleapis.com/storage/table_count`
- `bigquery.googleapis.com/storage/stored_bytes`
- `bigquery.googleapis.com/storage/uploaded_row_count`

Cach doc nhanh:
- Query count + execution time tang dong thoi -> tai tang that.
- Assigned slot gan max slot -> nguy co queue/cham.
- Stored bytes/uploaded rows dot bien -> check du lieu bat thuong/cost.

### Cloud Run dashboard (`monitoring/dashboard/Cloud Run Monitoring.json`)

- `run.googleapis.com/request_count`
- `run.googleapis.com/request_latencies`
- `run.googleapis.com/container/instance_count`
- `run.googleapis.com/container/billable_instance_time`
- `run.googleapis.com/container/cpu/allocation_time`
- `run.googleapis.com/container/memory/allocation_time`
- `run.googleapis.com/container/cpu/utilizations`
- `run.googleapis.com/container/memory/utilizations`

Cach doc nhanh:
- p95 latency tang nhung request count khong tang -> van de app/phu thuoc.
- 5xx / request_count tang -> uu tien incident.
- CPU/Mem util cao keo dai + instance_count cham tang -> can scale tuning.

### Composer dashboard (`monitoring/dashboard/Composer Ops.json`)

- `composer.googleapis.com/environment/dag_processing/total_parse_time`
- `composer.googleapis.com/environment/executor/queued_tasks`
- `composer.googleapis.com/environment/executor/running_tasks`
- `composer.googleapis.com/environment/scheduler/pod_eviction_count`
- `composer.googleapis.com/environment/worker/pod_eviction_count`
- `composer.googleapis.com/workload/cpu/usage_time`
- `composer.googleapis.com/workload/memory/bytes_used`
- `composer.googleapis.com/workflow/task/run_duration`

Cach doc nhanh:
- queued_tasks tang, running_tasks phang -> nghen executor.
- pod_eviction_count > 0 lap lai -> pressure tai nguyen/K8s.
- run_duration task tang deu -> regress DAG/dependency.

### Dataproc dashboard (`monitoring/dashboard/Dataproc Cluster [cluster-8649].json`)

- `dataproc.googleapis.com/cluster/yarn/memory_size`
- `dataproc.googleapis.com/cluster/yarn/pending_memory_size`
- `dataproc.googleapis.com/cluster/yarn/nodemanagers`
- `dataproc.googleapis.com/cluster/hdfs/storage_capacity`
- `compute.googleapis.com/instance/cpu/utilization`
- network bytes/packets in/out
- disk bytes/ops read/write

Cach doc nhanh:
- pending_memory_size tang + nodemanagers khong tang -> queue pressure.
- CPU cao + disk/network cao keo dai -> cluster sat nguong, can autoscale.

### Datastream dashboard (`monitoring/dashboard/DS latency.json`)

- `datastream.googleapis.com/stream/latencies`
- `datastream.googleapis.com/stream/freshness`

Cach doc nhanh:
- latency/freshness tang lien tuc -> stream backlog.
- can doi chieu voi custom `ops/datastream/error_events` de xac dinh fail hay chi tre.

## 4) Cach doc metric dung cho incident

- Buoc 1: Xac dinh pham vi (source ingest, orchestration, transform, serving).
- Buoc 2: Doc metric ratio truoc (failed_ratio, success_ratio), sau do moi doc count.
- Buoc 3: Dung p95/p99 de ra quyet dinh SLO, khong dung trung binh.
- Buoc 4: Khi metric custom = 0 bat thuong, check ngay:
  - scheduled query co data moi trong `ops_monitor.*`
  - exporter job co push point
  - dashboard filter `project_id` dung

## 5) Nguong canh bao goi y (tham chieu ban dau)

- Watermark stale > 180 phut
- BQ `failed_ratio` > 0.05 trong 15 phut
- Scheduler `success_ratio` < 0.95 trong 30 phut
- Datastream `error_events` > 0 lien tiep 15 phut
- DAG `restore_buffer_minutes < 0` (critical)
- DAG `recovery_time_minutes > restore_window_minutes` (critical)

Can calibrate theo baseline 2-4 tuan van hanh thuc te.
