
# from pyspark.sql.functions import (
#     col, lit, when, current_timestamp, input_file_name, split, conv
# )
# from pyspark.sql.types import (
#     StructType, TimestampType, LongType, IntegerType
# )

# # ----------------------------
# # schema helpers
# # ----------------------------
# def _get_field_type(schema, path: str):
#     parts = path.split(".")
#     cur = schema
#     for p in parts:
#         if not isinstance(cur, StructType):
#             return None
#         field = next((f for f in cur.fields if f.name == p), None)
#         if field is None:
#             return None
#         cur = field.dataType
#     return cur

# def _has_field(schema, path: str) -> bool:
#     return _get_field_type(schema, path) is not None

# # ----------------------------
# # timestamp helpers
# # ----------------------------
# def micros_to_ts(c): return (c / 1_000_000).cast("timestamp")
# def millis_to_ts(c): return (c / 1000).cast("timestamp")

# def _commit_ts_expr(df_raw):
#     sch = df_raw.schema
#     inferred = lit(False)

#     # 1) source_metadata.source_timestamp
#     dt = _get_field_type(sch, "source_metadata.source_timestamp")
#     if dt is not None:
#         if isinstance(dt, TimestampType):
#             return col("source_metadata.source_timestamp"), inferred
#         if isinstance(dt, (LongType, IntegerType)):
#             return micros_to_ts(col("source_metadata.source_timestamp")), inferred

#     # 2) root source_timestamp
#     dt = _get_field_type(sch, "source_timestamp")
#     if dt is not None:
#         if isinstance(dt, TimestampType):
#             return col("source_timestamp"), inferred
#         if isinstance(dt, (LongType, IntegerType)):
#             return micros_to_ts(col("source_timestamp")), inferred

#     # 3) read_timestamp
#     dt = _get_field_type(sch, "read_timestamp")
#     if dt is not None:
#         if isinstance(dt, TimestampType):
#             return col("read_timestamp"), lit(True)
#         if isinstance(dt, (LongType, IntegerType)):
#             return millis_to_ts(col("read_timestamp")), lit(True)

#     # 4) fallback
#     return current_timestamp(), lit(True)

# # ----------------------------
# # LSN helper
# # ----------------------------
# def lsn_to_num(lsn_col):
#     """
#     Convert Postgres LSN '0/FD541068' -> INT64
#     """
#     parts = split(lsn_col, "/")
#     xlog = conv(parts.getItem(0), 16, 10).cast("long")
#     offset = conv(parts.getItem(1), 16, 10).cast("long")
#     return xlog * lit(4294967296) + offset

# # ----------------------------
# # main normalizer
# # ----------------------------
# def normalize_datastream_cdc(df_raw, table: str, run_id: str, source_date_local: str):
#     sch = df_raw.schema
#     commit_expr, commit_inferred = _commit_ts_expr(df_raw)

#     op_expr = (
#         when(col("source_metadata.change_type") == "INSERT", lit("c"))
#         .when(col("source_metadata.change_type") == "UPDATE", lit("u"))
#         .when(col("source_metadata.change_type") == "DELETE", lit("d"))
#         .otherwise(lit(None))
#     )

#     df = df_raw.select(
#         col("payload.*"),
#         op_expr.alias("__op"),

#         col("source_metadata.lsn").alias("__lsn")
#         if _has_field(sch, "source_metadata.lsn") else lit(None).alias("__lsn"),

#         col("source_metadata.tx_id").alias("__tx_id")
#         if _has_field(sch, "source_metadata.tx_id") else lit(None).alias("__tx_id"),

#         col("source_metadata.schema").alias("__schema")
#         if _has_field(sch, "source_metadata.schema") else lit(None).alias("__schema"),

#         col("source_metadata.table").alias("__table")
#         if _has_field(sch, "source_metadata.table") else lit(None).alias("__table"),

#         col("source_metadata.is_deleted").cast("boolean").alias("__is_deleted")
#         if _has_field(sch, "source_metadata.is_deleted") else lit(False).alias("__is_deleted"),

#         commit_expr.alias("__commit_ts"),
#         commit_inferred.alias("__commit_ts_inferred"),

#         col("read_timestamp").alias("__read_ts")
#         if _has_field(sch, "read_timestamp") else lit(None).alias("__read_ts"),

#         col("stream_name").alias("__stream")
#         if _has_field(sch, "stream_name") else lit(None).alias("__stream"),

#         col("uuid").alias("__uuid")
#         if _has_field(sch, "uuid") else lit(None).alias("__uuid"),

#         current_timestamp().alias("__ingest_ts"),
#         lit(run_id).alias("__run_id"),
#         lit(source_date_local).alias("__source_date_local"),
#         input_file_name().alias("__raw_path"),
#     )

#     # add __lsn_num
#     df = df.withColumn(
#         "__lsn_num",
#         when(col("__lsn").isNotNull(), lsn_to_num(col("__lsn")))
#     )

#     # soft delete
#     if "deleted_at" in df.columns:
#         df = df.withColumn(
#             "__is_deleted",
#             col("__is_deleted") | col("deleted_at").isNotNull() | (col("__op") == "d")
#         )
#     else:
#         df = df.withColumn("__is_deleted", col("__is_deleted") | (col("__op") == "d"))

#     return df

from pyspark.sql.functions import (
    col, lit, when, current_timestamp, input_file_name, split, conv, regexp_replace,
    coalesce, crc32, regexp_extract, abs as sql_abs, trim
)


from pyspark.sql.types import (
    StructType, TimestampType, LongType, IntegerType
)

# ----------------------------
# schema helpers
# ----------------------------
def _get_field_type(schema, path: str):
    parts = path.split(".")
    cur = schema
    for p in parts:
        if not isinstance(cur, StructType):
            return None
        field = next((f for f in cur.fields if f.name == p), None)
        if field is None:
            return None
        cur = field.dataType
    return cur

def _has_field(schema, path: str) -> bool:
    return _get_field_type(schema, path) is not None

# ----------------------------
# timestamp helpers
# ----------------------------
def micros_to_ts(c):
    return (c / 1_000_000).cast("timestamp")

def millis_to_ts(c):
    return (c / 1000).cast("timestamp")

def _commit_ts_expr(df_raw):
    sch = df_raw.schema
    inferred = lit(False)

    # 1) source_metadata.source_timestamp
    dt = _get_field_type(sch, "source_metadata.source_timestamp")
    if dt is not None:
        if isinstance(dt, TimestampType):
            return col("source_metadata.source_timestamp"), inferred
        if isinstance(dt, (LongType, IntegerType)):
            return micros_to_ts(col("source_metadata.source_timestamp")), inferred

    # 2) root source_timestamp
    dt = _get_field_type(sch, "source_timestamp")
    if dt is not None:
        if isinstance(dt, TimestampType):
            return col("source_timestamp"), inferred
        if isinstance(dt, (LongType, IntegerType)):
            return micros_to_ts(col("source_timestamp")), inferred

    # 3) read_timestamp
    dt = _get_field_type(sch, "read_timestamp")
    if dt is not None:
        if isinstance(dt, TimestampType):
            return col("read_timestamp"), lit(True)
        if isinstance(dt, (LongType, IntegerType)):
            return millis_to_ts(col("read_timestamp")), lit(True)

    # 4) fallback
    return current_timestamp(), lit(True)

# ----------------------------
# LSN helper
# ----------------------------
def lsn_to_num(lsn_col):
    """
    Convert Postgres LSN '0/FD541068' -> INT64
    """
    parts = split(lsn_col, "/")
    xlog = conv(parts.getItem(0), 16, 10).cast("long")
    offset = conv(parts.getItem(1), 16, 10).cast("long")
    return xlog * lit(4294967296) + offset

# ----------------------------
# robust commit_ts -> numeric key
# ----------------------------
def commit_ts_to_key(ts_col):
    """
    Convert TIMESTAMP to a sortable INT64 key without unix_* or date_format.
    Uses Spark's timestamp -> string, then remove non-digits.

    Example: '2026-01-27 14:26:14.175' -> 20260127142614175
    """
    # cast timestamp to string: 'yyyy-MM-dd HH:mm:ss[.SSS]'
    s = ts_col.cast("string")
    # keep digits only
    digits = regexp_replace(s, r"[^0-9]", "")
    # ensure at least seconds: if no millis, still ok
    return digits.cast("long")

# ----------------------------
# main normalizer
# ----------------------------
def normalize_datastream_cdc(df_raw, table: str, run_id: str, source_date_local: str):
    sch = df_raw.schema
    commit_expr, commit_inferred = _commit_ts_expr(df_raw)

    # Datastream uses change_type (INSERT/UPDATE/DELETE)
    op_expr = (
        when(col("source_metadata.change_type") == "INSERT", lit("c"))
        .when(col("source_metadata.change_type") == "UPDATE", lit("u"))
        .when(col("source_metadata.change_type") == "DELETE", lit("d"))
        .otherwise(lit(None))
    )

    df = df_raw.select(
        col("payload.*"),
        op_expr.alias("__op"),

        col("source_metadata.lsn").alias("__lsn")
        if _has_field(sch, "source_metadata.lsn") else lit(None).alias("__lsn"),

        col("source_metadata.tx_id").alias("__tx_id")
        if _has_field(sch, "source_metadata.tx_id") else lit(None).alias("__tx_id"),

        col("source_metadata.schema").alias("__schema")
        if _has_field(sch, "source_metadata.schema") else lit(None).alias("__schema"),

        col("source_metadata.table").alias("__table")
        if _has_field(sch, "source_metadata.table") else lit(None).alias("__table"),

        col("source_metadata.is_deleted").cast("boolean").alias("__is_deleted")
        if _has_field(sch, "source_metadata.is_deleted") else lit(False).alias("__is_deleted"),

        commit_expr.alias("__commit_ts"),
        commit_inferred.alias("__commit_ts_inferred"),

        col("read_timestamp").alias("__read_ts")
        if _has_field(sch, "read_timestamp") else lit(None).alias("__read_ts"),

        col("stream_name").alias("__stream")
        if _has_field(sch, "stream_name") else lit(None).alias("__stream"),

        col("uuid").alias("__uuid")
        if _has_field(sch, "uuid") else lit(None).alias("__uuid"),

        current_timestamp().alias("__ingest_ts"),
        lit(run_id).alias("__run_id"),
        lit(source_date_local).alias("__source_date_local"),
        input_file_name().alias("__raw_path"),
    )

    # ----------------------------
    # add __lsn_num + __event_seq (BACKFILL-SAFE)
    # ----------------------------

    # Treat blank LSN as NULL (Datastream backfill often gives "")
    df = df.withColumn(
        "__lsn",
        when(trim(col("__lsn")) == lit(""), lit(None)).otherwise(col("__lsn"))
    )

    # Stable tie-breaker (always non-negative)
    tie_breaker = (
        sql_abs(crc32(coalesce(col("__uuid"), col("__raw_path")))) % lit(1_000_000)
    ).cast("long")

    # Try extract base id from filename (support multiple patterns)
    # Examples:
    #  - "...backfill_620870101_6_0.avro" -> 620870101
    #  - "...snapshot_123456_0.avro" -> 123456
    #  - "...initial_98765_0.avro" -> 98765
    base_str = regexp_extract(col("__raw_path"), r"(?:backfill_|snapshot_|initial_)(\d+)", 1)
    file_base = when(base_str != lit(""), base_str.cast("long"))

    # commit_ts numeric key fallback (sortable)
    commit_base = commit_ts_to_key(col("__commit_ts"))

    # Choose base for inferred ordering: file_base if present else commit_base
    inferred_base = coalesce(file_base, commit_base).cast("long")

    # Validate LSN format: "HEX/HEX"
    lsn_ok = col("__lsn").rlike(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")

    # Whether we used real LSN
    df = df.withColumn("__lsn_num_inferred", ~(col("__lsn").isNotNull() & lsn_ok))

    # __lsn_num:
    # - if real LSN exists & valid: parsed LSN
    # - else: inferred_base * 1e6 + tie_breaker  (never null)
    df = df.withColumn(
        "__lsn_num",
        when(col("__lsn").isNotNull() & lsn_ok, lsn_to_num(col("__lsn")))
        .otherwise(inferred_base * lit(1_000_000) + tie_breaker)
    )

    # Optional: __event_seq (alias for merge ordering)
    df = df.withColumn("__event_seq", col("__lsn_num"))


    # ----------------------------
    # soft delete
    # ----------------------------
    if "deleted_at" in df.columns:
        df = df.withColumn(
            "__is_deleted",
            col("__is_deleted") | col("deleted_at").isNotNull() | (col("__op") == "d")
        )
    else:
        df = df.withColumn("__is_deleted", col("__is_deleted") | (col("__op") == "d"))

    return df
