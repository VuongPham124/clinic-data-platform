from pyspark.sql.functions import col, lit, to_timestamp
from functools import reduce
from pyspark.sql.column import Column

SPARK_TYPE_MAP = {
    "string": "string",
    "long": "long",
    "int": "int",
    "double": "double",
    "boolean": "boolean"
}

def apply_schema_casting(df, table_cfg):
    for c, t in table_cfg.get("schema", {}).items():
        if c in df.columns:
            spark_type = SPARK_TYPE_MAP.get(t, t)
            df = df.withColumn(c, col(c).cast(spark_type))
    return df


# def apply_timestamp_casting(df, table_cfg):
#     for f in table_cfg.get("timestamp_fields", []):
#         if f in df.columns:
#             df = df.withColumn(f, (col(f) / 1_000_000).cast("timestamp"))
#     return df

from pyspark.sql.types import TimestampType, LongType, IntegerType

# def apply_timestamp_casting(df, table_cfg):
#     ts_fields = table_cfg.get("timestamp_fields", [])
#     for f in ts_fields:
#         if f in df.columns:
#             dt = df.schema[f].dataType
#             if isinstance(dt, TimestampType):
#                 continue
#             if isinstance(dt, (LongType, IntegerType)):
#                 df = df.withColumn(f, (col(f) / 1_000_000).cast("timestamp"))
#             else:
#                 # fallback: try cast directly
#                 df = df.withColumn(f, col(f).cast("timestamp"))
#     return df

def apply_timestamp_casting(df, table_cfg):
    ts_fields = table_cfg.get("timestamp_fields") or []
    for f in ts_fields:
        if f in df.columns:
            df = df.withColumn(f, to_timestamp(col(f)))  # ví dụ
    return df


# def build_invalid_condition(df, table_cfg):
#     conds = []

#     for pk in table_cfg.get("primary_key", ["id"]):
#         conds.append(col(pk).isNull())

#     for f in table_cfg.get("required_fields", []):
#         if f in df.columns:
#             conds.append(col(f).isNull())

#     conds.append(col("__lsn_num").isNull()) #check, can rm

#     if not conds:
#         return lit(False)

#     return reduce(lambda a, b: a | b, conds)



def _or_all(conds):
    if not conds:
        return lit(False)
    return reduce(lambda a, b: a | b, conds)

def build_invalid_condition(df, table_cfg) -> Column:
    """
    Contract-driven invalid condition:
    - PK must not be null
    - required_fields (if any) must not be null
    Everything else is allowed to be null.
    """
    pk = table_cfg.get("primary_key") or table_cfg.get("pk") or ["id"]
    required = table_cfg.get("required_fields") or []

    conds = []

    # 1) PK null => invalid
    for k in pk:
        if k in df.columns:
            conds.append(col(k).isNull())
        else:
            # missing PK column itself is invalid
            conds.append(lit(True))

    # 2) required fields null => invalid
    for f in required:
        if f in df.columns:
            conds.append(col(f).isNull())
        else:
            conds.append(lit(True))

    return _or_all(conds)
