import json
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

SPIKE_THRESHOLD = int(os.getenv("BOOKING_SPIKE_THRESHOLD", "120"))
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "public.clinic_bookings")
VALID_CDC_OPS = {"c", "u", "d", "r"}


def parse_msg(msg: bytes):
    try:
        return json.loads(msg.decode("utf-8"))
    except Exception:
        return None


def to_event(rec: dict):
    if not isinstance(rec, dict):
        return None

    payload = rec.get("payload", rec)
    if not isinstance(payload, dict):
        return None

    op = (payload.get("op") or "").lower()
    if op not in VALID_CDC_OPS:
        return None
    if op != "c":
        return None

    source = payload.get("source") or {}
    schema_name = (source.get("schema") or "").strip()
    table_name = (source.get("table") or "").strip()
    incoming_source_table = f"{schema_name}.{table_name}" if schema_name and table_name else SOURCE_TABLE
    if incoming_source_table.lower() != SOURCE_TABLE.lower():
        return None

    after = payload.get("after") or {}
    before = payload.get("before") or {}
    if not isinstance(after, dict):
        after = {}
    if not isinstance(before, dict):
        before = {}

    # Skip control/heartbeat messages that are not row-level booking changes.
    if not after and not before and op != "d":
        return None

    ts_ms = payload.get("ts_ms")

    if ts_ms:
        event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
    else:
        event_time = datetime.now(timezone.utc).isoformat()

    return {
        "source_table": incoming_source_table,
        "event_time": event_time,
        "op": op,
    }


class BuildAlertsFn(beam.DoFn):
    def process(self, kv, window=beam.DoFn.WindowParam):
        _, counts = kv
        total = int(counts.get("total", 0))
        if total <= 0:
            return

        window_start = window.start.to_utc_datetime().isoformat()
        window_end = window.end.to_utc_datetime().isoformat()
        base = {
            "source_table": SOURCE_TABLE,
            "window_start": window_start,
            "window_end": window_end,
            "event_time": window_end,
            "total_events_1m": total,
        }

        if total >= SPIKE_THRESHOLD:
            yield {
                **base,
                "alert_type": "booking_spike",
                "severity": "critical" if total >= SPIKE_THRESHOLD * 2 else "warning",
                "alert_key": f"booking_spike|{SOURCE_TABLE}",
            }


def run():
    opts = PipelineOptions(streaming=True, save_main_session=True)
    sub = os.getenv(
        "RAW_CDC_SUBSCRIPTION",
        "projects/wata-clinicdataplatform-gcp/subscriptions/raw-cdc-events-sub",
    )
    out_topic = os.getenv(
        "ALERT_TOPIC",
        "projects/wata-clinicdataplatform-gcp/topics/business-alerts",
    )

    with beam.Pipeline(options=opts) as p:
        events = (
            p
            | "ReadCDC" >> beam.io.ReadFromPubSub(subscription=sub)
            | "Parse" >> beam.Map(parse_msg)
            | "FilterValid" >> beam.Filter(lambda x: x is not None)
            | "Normalize" >> beam.Map(to_event)
            | "FilterNormalized" >> beam.Filter(lambda x: x is not None)
            | "Window1m" >> beam.WindowInto(FixedWindows(60))
        )

        alerts = (
            events
            | "ToCounts" >> beam.Map(lambda _: ("global", {"total": 1}))
            | "MergeCounts" >> beam.CombinePerKey(
                lambda rows: {
                    "total": sum(r["total"] for r in rows),
                }
            )
            | "BuildAlerts" >> beam.ParDo(BuildAlertsFn())
            | "ToJson" >> beam.Map(lambda x: json.dumps(x, separators=(",", ":")).encode("utf-8"))
        )

        _ = alerts | "PublishAlerts" >> beam.io.WriteToPubSub(topic=out_topic)


if __name__ == "__main__":
    run()
