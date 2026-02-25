import os
import re
from datetime import datetime, timezone
from typing import Dict, Iterable, List

from google.cloud import bigquery
from google.cloud import monitoring_v3
from google.api import metric_pb2
from google.api import monitored_resource_pb2
from google.api_core import exceptions as gexc
from google.protobuf.timestamp_pb2 import Timestamp


PROJECT_ID = os.getenv("PROJECT_ID", "wata-clinicdataplatform-gcp")
OPS_DATASET = os.getenv("OPS_DATASET", "ops_monitor")
LOCATION = os.getenv("LOCATION", "us-central1")
NAMESPACE = os.getenv("METRIC_NAMESPACE", "ops-monitor")
JOB = os.getenv("METRIC_JOB", "ops-monitor-exporter")
TASK_ID = os.getenv("METRIC_TASK_ID", "default")


def _now_ts() -> datetime:
    return datetime.now(timezone.utc)


def _resource() -> monitored_resource_pb2.MonitoredResource:
    # Use global resource to avoid strict label requirements and resource-type
    # mismatches that can invalidate large batches.
    return monitored_resource_pb2.MonitoredResource(
        type="global",
        labels={
            "project_id": PROJECT_ID,
        },
    )


def _rows(client: bigquery.Client, sql: str) -> Iterable[bigquery.table.Row]:
    return client.query(sql).result()


def _split_fqn(table_fqn: str) -> tuple[str, str, str]:
    clean = table_fqn.strip().strip("`")
    parts = clean.split(".")
    if len(parts) != 3 or any(not p for p in parts):
        raise ValueError(f"Invalid table fqn: {table_fqn}")
    return parts[0], parts[1], parts[2]


def _has_column(client: bigquery.Client, table_fqn: str, column_name: str) -> bool:
    project, dataset, table = _split_fqn(table_fqn)
    sql = f"""
    SELECT 1
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table_name
      AND column_name = @column_name
    LIMIT 1
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table),
            bigquery.ScalarQueryParameter("column_name", "STRING", column_name),
        ]
    )
    rows = list(client.query(sql, job_config=cfg).result())
    return len(rows) > 0


def _latest_where_clause(client: bigquery.Client, table_fqn: str, preferred_cols: List[str]) -> str:
    for c in preferred_cols:
        if _has_column(client, table_fqn, c):
            return f"WHERE {c} = (SELECT MAX({c}) FROM {table_fqn})"
    return ""


def _table_exists(client: bigquery.Client, table_fqn: str) -> bool:
    project, dataset, table = _split_fqn(table_fqn)
    table_id = f"{project}.{dataset}.{table}"
    try:
        client.get_table(table_id)
        print(f"table_exists: {table_id}=true")
        return True
    except Exception as e:
        print(f"table_exists: {table_id}=false error={e}")
        return False


def _write_points(
    client: monitoring_v3.MetricServiceClient,
    series: List[monitoring_v3.TimeSeries],
) -> None:
    if not series:
        return
    try:
        client.create_time_series(name=f"projects/{PROJECT_ID}", time_series=series)
        return
    except (gexc.ResourceExhausted, gexc.BadRequest, gexc.InvalidArgument, ValueError, Exception) as e:
        # If a batch fails with too much error metadata or mixed invalid points,
        # split recursively so one bad point does not fail the whole export.
        if len(series) == 1:
            s = series[0]
            print(
                "Skip invalid point",
                {"metric": s.metric.type, "labels": dict(s.metric.labels), "error": str(e)},
            )
            return
        mid = len(series) // 2
        _write_points(client, series[:mid])
        _write_points(client, series[mid:])


def _row_to_dict(row: bigquery.table.Row) -> Dict[str, object]:
    return {k: row[k] for k in row.keys()}


def _val(d: Dict[str, object], keys: List[str], default: float = 0) -> float:
    for k in keys:
        if k in d and d[k] is not None:
            return float(d[k])
    return float(default)


def _make_series(metric_type: str, value: float, labels: Dict[str, str]) -> monitoring_v3.TimeSeries:
    ts = monitoring_v3.TimeSeries()
    # Enforce custom metric label limits to avoid invalid-argument storms.
    safe_labels = {
        str(k)[:100]: str(v)[:100]
        for k, v in labels.items()
        if v is not None and str(v).strip() != ""
    }
    ts.metric = metric_pb2.Metric(type=metric_type, labels=safe_labels)
    ts.resource = _resource()
    point = monitoring_v3.Point()
    now = _now_ts()
    end_ts = Timestamp()
    end_ts.FromDatetime(now)
    point.interval.end_time = end_ts
    point.value.double_value = float(value)
    ts.points = [point]
    return ts


def _series_key(ts: monitoring_v3.TimeSeries) -> str:
    metric_labels = ",".join(f"{k}={v}" for k, v in sorted(ts.metric.labels.items()))
    resource_labels = ",".join(f"{k}={v}" for k, v in sorted(ts.resource.labels.items()))
    return f"{ts.metric.type}|{metric_labels}|{ts.resource.type}|{resource_labels}"


def _dedupe_series(series: List[monitoring_v3.TimeSeries]) -> List[monitoring_v3.TimeSeries]:
    # Keep the last point for each unique metric+labels+resource tuple.
    uniq: Dict[str, monitoring_v3.TimeSeries] = {}
    for s in series:
        uniq[_series_key(s)] = s
    return list(uniq.values())


def export_daily_watermark_health(bq: bigquery.Client) -> List[monitoring_v3.TimeSeries]:
    table_fqn = f"`{PROJECT_ID}.{OPS_DATASET}.daily_watermark_health`"
    if not _table_exists(bq, table_fqn):
        return []
    where_clause = _latest_where_clause(bq, table_fqn, ["monitor_date_local", "updated_at"])
    sql = f"""
    SELECT table_name, mins_since_watermark_update, mins_since_last_commit
    FROM {table_fqn}
    {where_clause}
    """
    out: List[monitoring_v3.TimeSeries] = []
    for r in _rows(bq, sql):
        labels = {"table_name": str(r["table_name"])}
        out.append(_make_series("custom.googleapis.com/ops/watermark/mins_since_update", r["mins_since_watermark_update"] or 0, labels))
        out.append(_make_series("custom.googleapis.com/ops/watermark/mins_since_last_commit", r["mins_since_last_commit"] or 0, labels))
    return out


def export_hourly_bq_job_health(bq: bigquery.Client) -> List[monitoring_v3.TimeSeries]:
    table_fqn = f"`{PROJECT_ID}.{OPS_DATASET}.hourly_bq_job_health`"
    if not _table_exists(bq, table_fqn):
        return []
    where_clause = _latest_where_clause(bq, table_fqn, ["hour_bucket", "monitor_ts_utc", "monitor_date_local"])
    sql = f"""
    SELECT *
    FROM {table_fqn}
    {where_clause}
    LIMIT 1
    """
    out: List[monitoring_v3.TimeSeries] = []
    rows = list(_rows(bq, sql))
    if not rows:
        return out
    r = _row_to_dict(rows[0])
    labels = {"scope": "all"}
    out.append(_make_series("custom.googleapis.com/ops/bq/jobs_total", _val(r, ["jobs_total", "total_jobs", "job_total"]), labels))
    out.append(_make_series("custom.googleapis.com/ops/bq/jobs_failed", _val(r, ["jobs_failed", "failed_jobs", "job_failed"]), labels))
    out.append(_make_series("custom.googleapis.com/ops/bq/failed_ratio", _val(r, ["failed_ratio", "job_failed_ratio"]), labels))
    out.append(_make_series("custom.googleapis.com/ops/bq/bytes_processed", _val(r, ["bytes_processed", "total_bytes_processed"]), labels))
    out.append(_make_series("custom.googleapis.com/ops/bq/slot_ms", _val(r, ["slot_ms", "total_slot_ms"]), labels))
    out.append(_make_series("custom.googleapis.com/ops/bq/p95_duration_sec", _val(r, ["p95_duration_sec", "duration_p95_sec", "p95_sec"]), labels))
    return out


def export_hourly_composer_airflow_health(bq: bigquery.Client) -> List[monitoring_v3.TimeSeries]:
    table_fqn = f"`{PROJECT_ID}.{OPS_DATASET}.hourly_composer_airflow_health`"
    if not _table_exists(bq, table_fqn):
        return []
    where_clause = _latest_where_clause(bq, table_fqn, ["hour_bucket", "monitor_ts_utc", "monitor_date_local"])
    sql = f"""
    SELECT
      component,
      SUM(COALESCE(total_events, 0)) AS total_events,
      SUM(COALESCE(error_events, 0)) AS error_events,
      SUM(COALESCE(error_hints_text, 0)) AS error_hints_text
    FROM {table_fqn}
    {where_clause}
    GROUP BY component
    """
    out: List[monitoring_v3.TimeSeries] = []
    for row in _rows(bq, sql):
        r = _row_to_dict(row)
        labels = {"component": str(r.get("component", "unknown"))}
        out.append(_make_series("custom.googleapis.com/ops/composer/total_events", _val(r, ["total_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/composer/error_events", _val(r, ["error_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/composer/error_hints_text", _val(r, ["error_hints_text"]), labels))
    return out


def export_hourly_datastream_health(bq: bigquery.Client) -> List[monitoring_v3.TimeSeries]:
    table_fqn = f"`{PROJECT_ID}.{OPS_DATASET}.hourly_datastream_health`"
    if not _table_exists(bq, table_fqn):
        return []
    where_clause = _latest_where_clause(bq, table_fqn, ["hour_bucket", "monitor_ts_utc", "monitor_date_local"])
    sql = f"""
    SELECT *
    FROM {table_fqn}
    {where_clause}
    """
    out: List[monitoring_v3.TimeSeries] = []
    for row in _rows(bq, sql):
        r = _row_to_dict(row)
        labels = {"stream_id": str(r.get("stream_id", "unknown"))}
        out.append(_make_series("custom.googleapis.com/ops/datastream/total_events", _val(r, ["total_events", "events_total"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/datastream/error_events", _val(r, ["error_events", "events_error"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/datastream/lag_or_error_hints", _val(r, ["lag_or_error_hints", "error_hints"]), labels))
    return out


def export_hourly_gcs_ingestion(bq: bigquery.Client) -> List[monitoring_v3.TimeSeries]:
    table_fqn = f"`{PROJECT_ID}.{OPS_DATASET}.hourly_gcs_ingestion`"
    if not _table_exists(bq, table_fqn):
        return []
    where_clause = _latest_where_clause(bq, table_fqn, ["hour_bucket", "monitor_ts_utc", "monitor_date_local"])
    sql = f"""
    SELECT *
    FROM {table_fqn}
    {where_clause}
    """
    out: List[monitoring_v3.TimeSeries] = []
    for row in _rows(bq, sql):
        r = _row_to_dict(row)
        labels = {
            "bucket_name": str(r.get("bucket_name", "unknown") or "unknown"),
            "table_id": str(r.get("table_id", "unknown") or "unknown"),
        }
        out.append(_make_series("custom.googleapis.com/ops/gcs/objects_created", _val(r, ["objects_created", "created_objects"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/gcs/objects_deleted", _val(r, ["objects_deleted", "deleted_objects"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/gcs/error_events", _val(r, ["error_events", "events_error"]), labels))
    return out


def export_hourly_cloud_scheduler_health(bq: bigquery.Client) -> List[monitoring_v3.TimeSeries]:
    table_fqn = f"`{PROJECT_ID}.{OPS_DATASET}.hourly_cloud_scheduler_health`"
    if not _table_exists(bq, table_fqn):
        return []
    where_clause = _latest_where_clause(bq, table_fqn, ["hour_bucket", "latest_event_ts", "monitor_ts_utc", "monitor_date_local"])
    sql = f"""
    SELECT *
    FROM {table_fqn}
    {where_clause}
    """
    out: List[monitoring_v3.TimeSeries] = []
    for row in _rows(bq, sql):
        r = _row_to_dict(row)
        labels = {
            "job_id": str(r.get("job_id", "unknown") or "unknown"),
            "scheduler_location": str(r.get("scheduler_location", "unknown") or "unknown"),
        }
        out.append(_make_series("custom.googleapis.com/ops/scheduler/total_events", _val(r, ["total_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/scheduler/error_events", _val(r, ["error_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/scheduler/http_4xx_5xx_events", _val(r, ["http_4xx_5xx_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/scheduler/http_5xx_events", _val(r, ["http_5xx_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/scheduler/success_events", _val(r, ["success_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/scheduler/non_success_events", _val(r, ["non_success_events"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/scheduler/success_ratio", _val(r, ["success_ratio"]), labels))
    return out


def export_daily_critical_dag_slo(bq: bigquery.Client) -> List[monitoring_v3.TimeSeries]:
    table_fqn = f"`{PROJECT_ID}.{OPS_DATASET}.daily_critical_dag_slo`"
    if not _table_exists(bq, table_fqn):
        return []
    where_clause = _latest_where_clause(bq, table_fqn, ["monitor_date_local", "monitor_ts_utc"])
    sql = f"""
    SELECT *
    FROM {table_fqn}
    {where_clause}
    """
    out: List[monitoring_v3.TimeSeries] = []
    for row in _rows(bq, sql):
        r = _row_to_dict(row)
        labels = {"dag_id": str(r.get("dag_id", "unknown") or "unknown")}
        out.append(_make_series("custom.googleapis.com/ops/dag/critical_p95_minutes", _val(r, ["critical_p95_minutes"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/dag/recovery_time_minutes", _val(r, ["recovery_time_minutes"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/dag/restore_buffer_minutes", _val(r, ["restore_buffer_minutes"]), labels))
        out.append(_make_series("custom.googleapis.com/ops/dag/restore_window_minutes", _val(r, ["restore_window_minutes"]), labels))
    return out


def main() -> None:
    bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    m = monitoring_v3.MetricServiceClient()

    all_series: List[monitoring_v3.TimeSeries] = []
    print(
        f"Exporter config: PROJECT_ID={PROJECT_ID}, OPS_DATASET={OPS_DATASET}, LOCATION={LOCATION}, "
        f"NAMESPACE={NAMESPACE}, JOB={JOB}, TASK_ID={TASK_ID}"
    )

    watermark_series = export_daily_watermark_health(bq)
    print(f"daily_watermark_health points: {len(watermark_series)}")
    all_series.extend(watermark_series)

    bq_job_series = export_hourly_bq_job_health(bq)
    print(f"hourly_bq_job_health points: {len(bq_job_series)}")
    all_series.extend(bq_job_series)

    composer_series = export_hourly_composer_airflow_health(bq)
    print(f"hourly_composer_airflow_health points: {len(composer_series)}")
    all_series.extend(composer_series)

    datastream_series = export_hourly_datastream_health(bq)
    print(f"hourly_datastream_health points: {len(datastream_series)}")
    all_series.extend(datastream_series)

    gcs_series = export_hourly_gcs_ingestion(bq)
    print(f"hourly_gcs_ingestion points: {len(gcs_series)}")
    all_series.extend(gcs_series)

    scheduler_series = export_hourly_cloud_scheduler_health(bq)
    print(f"hourly_cloud_scheduler_health points: {len(scheduler_series)}")
    all_series.extend(scheduler_series)

    dag_slo_series = export_daily_critical_dag_slo(bq)
    print(f"daily_critical_dag_slo points: {len(dag_slo_series)}")
    all_series.extend(dag_slo_series)
    all_series = _dedupe_series(all_series)
    print(f"total points after dedupe: {len(all_series)}")

    # Use smaller batches to reduce blast radius on partial failures.
    batch_size = 50
    for i in range(0, len(all_series), batch_size):
        _write_points(m, all_series[i : i + batch_size])

    print(f"Exported {len(all_series)} time series points to custom metrics.")


if __name__ == "__main__":
    main()
