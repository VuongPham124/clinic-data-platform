# import argparse
# from pyspark.sql import SparkSession
# from contract_loader import load_contract
# from cdc_engine import run_all_tables

# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--tables", required=True)
#     p.add_argument("--contract-path", required=True)
#     p.add_argument("--source-date-local")
#     p.add_argument("--month")
#     p.add_argument("--latest", action="store_true")
#     p.add_argument("--run-id", required=True)
#     p.add_argument("--raw-bucket", required=True)
#     p.add_argument("--staging-bucket", required=True)
#     p.add_argument("--quarantine-bucket", required=True)
#     p.add_argument("--ok-files", type=int, default=4)
#     p.add_argument("--bad-files", type=int, default=1)
#     return p.parse_args()

# def main():
#     args = parse_args()

#     spark = SparkSession.builder.appName("cdc_raw_to_staging").getOrCreate()
#     spark.conf.set("spark.sql.session.timeZone", "UTC")  # giữ UTC ở tầng kỹ thuật

#     # === BigQuery-friendly Parquet timestamps ===
#     spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
#     spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
#     spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
#     spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
#     spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
#     contract = load_contract(args.contract_path)

#     run_all_tables(spark, contract, args)

#     spark.stop()

# if __name__ == "__main__":
#     main()

import argparse
import re  # ADDED
from datetime import datetime  # ADDED

try:  # ADDED
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # ADDED
    ZoneInfo = None  # ADDED

from pyspark.sql import SparkSession
from contract_loader import load_contract
from cdc_engine import run_all_tables


# =========================
# ADDED: run_id convention helpers (daily)
# Convention: <env>_<cadence>_<yyyymmdd>T<hhmmss>_<tz>_<trigger>
# Example: prod_d_20260201T020000_ict_sched
# =========================
def sanitize_run_id(s: str) -> str:
    """Keep only [a-z0-9_], convert other chars to underscore, collapse repeats."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def make_run_id(env: str, cadence: str, tz_tag: str, trigger: str, tz_name: str) -> str:
    """Generate run_id using timezone-aware timestamp for ops readability."""
    if ZoneInfo is not None:
        now = datetime.now(ZoneInfo(tz_name))
    else:
        # Fallback if zoneinfo not available
        now = datetime.utcnow()
        tz_tag = "utc"
    ts = now.strftime("%Y%m%dT%H%M%S")
    rid = f"{env}_{cadence}_{ts}_{tz_tag}_{trigger}"
    return sanitize_run_id(rid)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--tables", required=True)
    p.add_argument("--contract-path", required=True)
    p.add_argument("--source-date-local")
    p.add_argument("--month")
    p.add_argument("--latest", action="store_true")

    # CHANGED: run-id is no longer required; auto-generated if missing  # ADDED
    p.add_argument("--run-id", required=False, default=None)  # ADDED

    # ADDED: run_id convention knobs (daily defaults)
    p.add_argument("--env", default="prod", help="Environment tag for run_id (prod|test|dev)")  # ADDED
    p.add_argument("--cadence", default="d", help="Cadence tag for run_id. Use 'd' for daily.")  # ADDED
    p.add_argument(
        "--trigger",
        default="manual",
        help="Trigger tag for run_id (sched|manual|rerun|backfill)"
    )  # ADDED
    p.add_argument("--tz-tag", default="ict", help="Timezone tag label in run_id (ict|utc)")  # ADDED
    p.add_argument(
        "--tz-name",
        default="Asia/Ho_Chi_Minh",
        help="Timezone name used to generate run_id timestamp"
    )  # ADDED

    p.add_argument("--raw-bucket", required=True)
    p.add_argument("--staging-bucket", required=True)
    p.add_argument("--quarantine-bucket", required=True)
    p.add_argument("--ok-files", type=int, default=4)
    p.add_argument("--bad-files", type=int, default=1)
    return p.parse_args()


def main():
    args = parse_args()

    # ADDED: normalize / auto-generate run_id for daily job
    if args.run_id:
        args.run_id = sanitize_run_id(args.run_id)
    else:
        args.run_id = make_run_id(
            env=args.env,
            cadence=args.cadence,
            tz_tag=args.tz_tag,
            trigger=args.trigger,
            tz_name=args.tz_name,
        )
    print(f"[INFO] Effective run_id = {args.run_id}")

    spark = SparkSession.builder.appName("cdc_raw_to_staging").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # giữ UTC ở tầng kỹ thuật

    # === BigQuery-friendly Parquet timestamps ===
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    contract = load_contract(args.contract_path)

    run_all_tables(spark, contract, args)

    spark.stop()


if __name__ == "__main__":
    main()
