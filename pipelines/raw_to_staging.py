import json
import logging
from datetime import datetime, timezone
from typing import Dict, List
from google.cloud import storage

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import length
from pyspark.sql.functions import (
    col, trim, lit, when,
    to_timestamp, to_date, row_number, substring, split
)

# ======================================================
# Logging
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("normalize_snapshot")

# ======================================================
# Contract
# ======================================================
def read_json(path: str) -> Dict:
    if path.startswith("gs://"):

        client = storage.Client()
        bucket_name, blob_path = path[5:].split("/", 1)
        blob = client.bucket(bucket_name).blob(blob_path)

        return json.loads(blob.download_as_text())
    else:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

# ======================================================
# GCS helpers
# ======================================================
def gcs_has_files(spark, glob_path: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    fs = Path(glob_path).getFileSystem(hconf)
    return bool(fs.globStatus(Path(glob_path)))

# ======================================================
# Dedup helpers
# ======================================================
def dedupe_by_pk(df, pk_cols: List[str], order_cols: List[str]):
    if not pk_cols:
        return df

    order_exprs = [
        col(c).desc()
        for c in order_cols
        if c in df.columns
    ]

    if not order_exprs:
        return df.dropDuplicates(pk_cols)

    w = Window.partitionBy(*pk_cols).orderBy(*order_exprs)

    return (
        df
        .withColumn("__rn", row_number().over(w))
        .filter(col("__rn") == 1)
        .drop("__rn")
    )

# ======================================================
# Transform helpers
# ======================================================
def trim_string_columns_by_contract(df, contract_cols: Dict[str, str]):
    for c, t in contract_cols.items():
        if c in df.columns and t.lower().startswith("string"):
            df = df.withColumn(c, trim(col(c)))
    return df

def ensure_columns(df, contract_cols: Dict[str, str]):
    for c in contract_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast("string"))
    return df

INVALID_TS = "0001-01-01 00:00:00.000000"

def cast_by_contract(df, contract_cols):
    for c, t in contract_cols.items():
        if c not in df.columns:
            continue

        tt = t.lower()

        # ---------- TIMESTAMP ----------
        if tt.startswith("timestamp"):
            year = split(col(c), "-").getItem(0).cast("int")

            df = df.withColumn(
                c,
                when(col(c).isNull(), None)          
                .when(col(c) == "", None)                  
                .when(year.isNull(), INVALID_TS)           
                .when(year < 1, INVALID_TS)
                .when(year > 9999, INVALID_TS)
                .otherwise(
                    to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss.SSSSSS")
                )
            )

        # ---------- DATE ----------
        elif tt == "date":
            df = df.withColumn(
                c,
                when(col(c).isNull() | (col(c) == ""), None)
                .otherwise(col(c).cast(DateType()))
            )

        # ---------- DECIMAL ----------
        elif tt.startswith("decimal"):
            df = df.withColumn(c, col(c).cast(tt))

        # ---------- OTHER ----------
        else:
            df = df.withColumn(c, col(c).cast(tt))

    return df

# ======================================================
# Validation helpers
# ======================================================
def build_validations(
    df,
    pk_cols: List[str],
    required_cols: List[str],
    min_date: str
):
    pk_ok = None
    for c in pk_cols:
        cond = col(c).isNotNull() if c in df.columns else lit(False)
        pk_ok = cond if pk_ok is None else (pk_ok & cond)
    if pk_ok is None:
        pk_ok = lit(True)

    req_ok = None
    for c in required_cols:
        cond = col(c).isNotNull() if c in df.columns else lit(False)
        req_ok = cond if req_ok is None else (req_ok & cond)
    if req_ok is None:
        req_ok = lit(True)

    ts_cols = [c for c, t in df.dtypes if t == "timestamp"]

    invalid_ts = None
    for c in ts_cols:
        expr = col(c).isNotNull() & (col(c) == INVALID_TS)
        invalid_ts = expr if invalid_ts is None else (invalid_ts | expr)

    if invalid_ts is None:
        invalid_ts = lit(False)

    ok = pk_ok & req_ok & (~invalid_ts)

    reject_reason = (
        when(~pk_ok, lit("missing_pk"))
        .when(~req_ok, lit("missing_required"))
        .when(~invalid_ts, lit("invalid_datetime"))
        .otherwise(lit("unknown"))
    )

    return ok, reject_reason

# ======================================================
# Main
# ======================================================
if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "normalize_snapshot_source_history"
    ).getOrCreate()
    
    spark.conf.set(
    "spark.sql.parquet.outputTimestampType",
    "TIMESTAMP_MICROS"
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    # -------- Buckets --------
    RAW_BUCKET = spark.conf.get("spark.raw.bucket", "amaz-raw")
    STAGING_BUCKET = spark.conf.get("spark.staging.bucket", "amaz-staging")
    QUARANTINE_BUCKET = spark.conf.get("spark.quarantine.bucket", "amaz-quarantine")

    RAW_PREFIX = "snapshot"
    STAGING_PREFIX = "snapshot"
    QUAR_PREFIX = "raw-to-staging/snapshot"

    # -------- Contract --------
    CONTRACT_FILE = spark.conf.get(
        "spark.normalize.contract_path",
        "staging_contract_snapshot_v1.json"
    )
    contract = read_json(CONTRACT_FILE)
    tables_cfg = contract["tables"]
    min_date = contract.get("timestamp_min_date", "1900-01-01")

    # -------- Detect run --------
    load_date = spark.conf.get("spark.normalize.load_date", "2026-01-26")
    run_id = spark.conf.get("spark.normalize.run_id", "a03c6115")

    logger.info(f"Detected RAW snapshot run load_date={load_date}, run_id={run_id}")

    started_at = datetime.now(timezone.utc).isoformat()

    total_ok = 0
    total_bad = 0
    tables_processed = 0

    pipeline_commit_ts = datetime.now(timezone.utc)
    SNAPSHOT_LSN = 1000

    # -------- Process tables --------
    for full_name, cfg in tables_cfg.items():
        logger.info(f"Start normalize table={full_name}")

        src = (
            f"gs://{RAW_BUCKET}/{RAW_PREFIX}/"
            f"{full_name}/load_date={load_date}/run_id={run_id}/*.csv.gz"
        )

        out_ok = (
            f"gs://{STAGING_BUCKET}/{STAGING_PREFIX}/"
            f"{full_name}/load_date={load_date}/run_id={run_id}/"
        )

        out_bad = (
            f"gs://{QUARANTINE_BUCKET}/{QUAR_PREFIX}/"
            f"{full_name}/load_date={load_date}/run_id={run_id}/"
        )

        try:
            if not gcs_has_files(spark, src):
                logger.warning(f"SKIP table={full_name} path not found")
                continue

            df = spark.read.option("header", "true").option("inferSchema", "false").csv(src)

            df = trim_string_columns_by_contract(df, cfg["columns"])
            df = ensure_columns(df, cfg["columns"])
            df = cast_by_contract(df, cfg["columns"])

            if "deleted_at" in df.columns:
                df = df.withColumn("is_deleted", col("deleted_at").isNotNull())
            else:
                df = df.withColumn("is_deleted", lit(False))

            df = (
                df
                .withColumn("__lsn_num", lit(SNAPSHOT_LSN))
                .withColumn("__commit_ts", lit(pipeline_commit_ts))
            )

            ok_cond, reject_reason = build_validations(
                df,
                cfg.get("primary_key", []),
                cfg.get("required", []),
                min_date
            )

            ok_df = dedupe_by_pk(
                df.filter(ok_cond),
                pk_cols=cfg.get("primary_key", []),
                order_cols=["updated_at", "created_at"]
            )
            bad_df = (
                df.filter(~ok_cond)
                  .withColumn("reject_reason", reject_reason)
            )

            ok_cnt = ok_df.count()
            bad_cnt = bad_df.count()

            total_ok += ok_cnt
            total_bad += bad_cnt

            ok_df.write.mode("overwrite").parquet(out_ok)
            bad_df.write.mode("overwrite").parquet(out_bad)

            tables_processed += 1

            logger.info(
                f"Finished table={full_name} | ok={ok_cnt} | quarantine={bad_cnt}"
            )

        except Exception as e:
            logger.error(
                f"FAILED table={full_name} error={str(e)}",
                exc_info=True
            )

    finished_at = datetime.now(timezone.utc).isoformat()

    # -------- SOURCE-LEVEL HISTORY --------
    history_record = {
        "source": "snapshot",
        "pipeline": "raw_to_staging",
        "load_date": load_date,
        "run_id": run_id,
        "status": "SUCCESS",
        "started_at": started_at,
        "finished_at": finished_at,
        "tables_processed": tables_processed,
        "total_rows_ok": total_ok,
        "total_rows_quarantine": total_bad
    }

    history_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    history_path = (
        f"{STAGING_PREFIX}/_HISTORY/"
        f"load_date={load_date}/"
        f"{run_id}/"
        f"history_{history_ts}.json"
    )

    client = storage.Client()
    bucket = client.bucket(STAGING_BUCKET)

    bucket.blob(history_path).upload_from_string(
        json.dumps(history_record, indent=2),
        content_type="application/json"
    )

    logger.info(f"History appended: gs://{STAGING_BUCKET}/{history_path}")

    logger.info(
        f"Snapshot normalize finished | tables={tables_processed} "
        f"| ok={total_ok} | quarantine={total_bad}"
    )

    spark.stop()