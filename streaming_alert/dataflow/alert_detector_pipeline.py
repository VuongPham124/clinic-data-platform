import json
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

SPIKE_THRESHOLD = int(os.getenv("BOOKING_SPIKE_THRESHOLD", "120"))
CANCEL_RATIO_THRESHOLD = float(os.getenv("CANCEL_RATIO_THRESHOLD", "0.40"))
MIN_SAMPLE_FOR_RATIO = int(os.getenv("MIN_SAMPLE_FOR_RATIO", "20"))
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "public.clinic_bookings")


def parse_msg(msg: bytes):
    try:
        return json.loads(msg.decode("utf-8"))
    except Exception:
        return None


def to_event(rec: dict):
    payload = rec.get("payload", rec)
    op = (payload.get("op") or "").lower()
    after = payload.get("after") or {}
    ts_ms = payload.get("ts_ms")

    if ts_ms:
        event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
    else:
        event_time = datetime.now(timezone.utc).isoformat()

    canceled_at = after.get("canceled_at")
    is_cancel = bool(canceled_at) or op == "d"
    return {
        "source_table": SOURCE_TABLE,
        "event_time": event_time,
        "op": op,
        "is_cancel": is_cancel,
    }


class BuildAlertsFn(beam.DoFn):
    def process(self, kv, window=beam.DoFn.WindowParam):
        _, counts = kv
        total = int(counts.get("total", 0))
        cancel = int(counts.get("cancel", 0))
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
            "cancel_events_1m": cancel,
        }

        if total >= SPIKE_THRESHOLD:
            yield {
                **base,
                "alert_type": "booking_spike",
                "severity": "critical" if total >= SPIKE_THRESHOLD * 2 else "warning",
                "alert_key": f"booking_spike|{SOURCE_TABLE}|{window_start}",
            }

        cancel_ratio = float(cancel) / float(total)
        if total >= MIN_SAMPLE_FOR_RATIO and cancel_ratio >= CANCEL_RATIO_THRESHOLD:
            yield {
                **base,
                "alert_type": "booking_cancel_ratio_spike",
                "severity": "critical" if cancel_ratio >= (CANCEL_RATIO_THRESHOLD + 0.2) else "warning",
                "cancel_ratio_1m": cancel_ratio,
                "alert_key": f"booking_cancel_ratio_spike|{SOURCE_TABLE}|{window_start}",
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
            | "Window1m" >> beam.WindowInto(FixedWindows(60))
        )

        alerts = (
            events
            | "ToCounts" >> beam.Map(
                lambda e: ("global", {"total": 1, "cancel": 1 if e["is_cancel"] else 0})
            )
            | "MergeCounts" >> beam.CombinePerKey(
                lambda rows: {
                    "total": sum(r["total"] for r in rows),
                    "cancel": sum(r["cancel"] for r in rows),
                }
            )
            | "BuildAlerts" >> beam.ParDo(BuildAlertsFn())
            | "ToJson" >> beam.Map(lambda x: json.dumps(x, separators=(",", ":")).encode("utf-8"))
        )

        _ = alerts | "PublishAlerts" >> beam.io.WriteToPubSub(topic=out_topic)


if __name__ == "__main__":
    run()
