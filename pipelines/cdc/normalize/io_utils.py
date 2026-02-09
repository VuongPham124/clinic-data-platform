import json, tempfile, os
from datetime import datetime
from pyspark.sql.functions import max as spark_max

def resolve_input_prefixes(raw_bucket, table, source_date, month, latest):
    base = f"{raw_bucket}/cdc/{table.replace('.', '_')}/"

    if source_date:
        y, m, d = source_date.split("-")
        return [f"{base}{y}/{m}/{d}/*/*"]

    if month:
        y, m = month.split("-")
        return [f"{base}{y}/{m}/*/*/*"]

    if latest:
        return [f"{base}*/*/*/*/*"]

    raise ValueError("Must provide source_date_local, month, or latest")


def write_manifest(path: str, payload: dict):
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        json.dump(payload, f, indent=2)
        tmp = f.name
    os.system(f"gsutil cp {tmp} {path}")

from pyspark.sql.functions import col, lit, when, current_timestamp, input_file_name

def has_col(df, name: str) -> bool:
    try:
        df.select(col(name))
        return True
    except Exception:
        return False

def millis_to_ts(c): return (c / 1000).cast("timestamp")
def micros_to_ts(c): return (c / 1_000_000).cast("timestamp")

def normalize_datastream_cdc(df_raw, table: str, run_id: str, source_date_local: str):
    # determine commit ts source
    commit_expr = None
    commit_inferred = lit(False)

    if has_col(df_raw, "source_metadata.source_timestamp"):
        commit_expr = micros_to_ts(col("source_metadata.source_timestamp"))
    elif has_col(df_raw, "source_timestamp"):
        # some variants put it at root
        commit_expr = micros_to_ts(col("source_timestamp"))
    elif has_col(df_raw, "read_timestamp"):
        # usually millis
        commit_expr = millis_to_ts(col("read_timestamp"))
        commit_inferred = lit(True)
    else:
        commit_expr = current_timestamp()
        commit_inferred = lit(True)

    df = df_raw.select(
        "payload.*",

        when(col("source_metadata.change_type") == "INSERT", lit("c"))
        .when(col("source_metadata.change_type") == "UPDATE", lit("u"))
        .when(col("source_metadata.change_type") == "DELETE", lit("d"))
        .alias("__op"),

        col("source_metadata.lsn").alias("__lsn"),
        col("source_metadata.tx_id").alias("__tx_id"),
        col("source_metadata.schema").alias("__schema"),
        col("source_metadata.table").alias("__table"),
        col("source_metadata.is_deleted").alias("__is_deleted"),

        commit_expr.alias("__commit_ts"),
        commit_inferred.alias("__commit_ts_inferred"),

        col("read_timestamp").alias("__read_ts") if has_col(df_raw, "read_timestamp") else lit(None).alias("__read_ts"),
        col("stream_name").alias("__stream") if has_col(df_raw, "stream_name") else lit(None).alias("__stream"),
        col("uuid").alias("__uuid") if has_col(df_raw, "uuid") else lit(None).alias("__uuid"),

        current_timestamp().alias("__ingest_ts"),
        lit(run_id).alias("__run_id"),
        lit(source_date_local).alias("__source_date_local"),
        input_file_name().alias("__raw_path")
    )

    # Soft delete: prefer metadata is_deleted, else deleted_at, else op
    if "deleted_at" in df.columns:
        df = df.withColumn(
            "__is_deleted",
            col("__is_deleted").cast("boolean") | col("deleted_at").isNotNull() | (col("__op") == "d")
        )
    else:
        df = df.withColumn("__is_deleted", col("__is_deleted").cast("boolean") | (col("__op") == "d"))

    return df

from urllib.parse import urlparse

def gcs_path_exists(spark, path_glob: str) -> bool:
    """
    Check existence for GCS path (gs://...) safely on Dataproc.
    We only check the prefix before wildcard to avoid deep listing.
    """
    prefix = path_glob.split("*")[0].rstrip("/")
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()

    Path = jvm.org.apache.hadoop.fs.Path
    p = Path(prefix)

    # ✅ IMPORTANT: choose FS by the path's URI scheme (gs://)
    fs = p.getFileSystem(hconf)

    return fs.exists(p)
