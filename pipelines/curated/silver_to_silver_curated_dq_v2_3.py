# silver_to_silver_curated_dq_v2_2_1.py
# Patch on top of your v2.2 to use FULL rules JSON (from Excel) while keeping v2 behavior.
#
# ==============================
# CHANGELOG v2.2.1 (ADD/COMMENT)
# ==============================
# - CHANGED: Use rules JSON as the single source of truth (no IMPORTANT_DQ_RULES hardcode).
# - ADDED: Robust rule table lookup by multiple aliases:
#   clinic_doctors | public_clinic_doctors | public.clinic_doctors | clinics vs public_clinics, etc.
# - FIXED: RANGE check now flags BOTH:
#     (1) parsed numeric out of range
#     (2) non-null raw but cannot be parsed to double (parse_fail)
#   -> This is the main reason v3 had bad=0 for public_clinics.
# - KEPT: v2 behavior: do NOT apply prose filters. Log only.
# - ADDED (optional but recommended): align bad_df schema to existing quarantine table before append
#   to avoid "schema mismatch: STRING vs INTEGER" errors.
# - KEPT: v2 performance optimizations (persist/repartition/1-pass metrics).

import argparse
import json
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------
# Contract + mapping helpers
# -----------------------------
def load_contract(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def contract_table_to_bq_name(contract_table: str) -> str:
    # "public.clinic_doctors" -> "public_clinic_doctors"
    return contract_table.replace(".", "_")


# -----------------------------
# Rules JSON loader (supports both shapes)
# -----------------------------
def load_rules_json(path: str) -> dict:
    """
    Supports 2 shapes:
    A) Old:
      { "tables": { "clinic_doctors": { "filters": [...], "columns": {...} } } }

    B) New (from Excel full):
      { "tables": { "clinic_doctors": { "filters_desc": [...], "filters_sql": [...], "columns": {...} } } }
    """
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("tables", {})


# -----------------------------
# Basic string normalization
# -----------------------------
def normalize_string_columns(df):
    # trim + empty string -> null (global policy) (KEPT v2)
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
            df = df.withColumn(c, F.when(F.length(F.col(c)) == 0, F.lit(None)).otherwise(F.col(c)))
    return df


# -----------------------------
# Null policy application
# -----------------------------
def apply_null_policies(df, col_rules: dict):
    """
    Policies supported (KEPT v2):
      - {"mode":"FILL_STRING","value":"UNKNOWN"}
      - {"mode":"FILL_NUMBER","value":-1}
      - others: no fill (enforced by DQ, not by fill)
    """
    out = df
    for col, rr in (col_rules or {}).items():
        if col not in out.columns:
            continue

        pol = (rr or {}).get("null_policy") or {}
        mode = (pol.get("mode") or "ALLOW_NULL").upper()

        if mode == "FILL_STRING":
            val = pol.get("value", "UNKNOWN")
            out = out.withColumn(col, F.when(F.col(col).isNull(), F.lit(val)).otherwise(F.col(col)))
        elif mode == "FILL_NUMBER":
            val = pol.get("value", -1)
            out = out.withColumn(col, F.when(F.col(col).isNull(), F.lit(val)).otherwise(F.col(col)))

    return out


# -----------------------------
# DQ checks helpers
# -----------------------------
def parse_range_token(token: str):
    m = re.match(r"range\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]", token.strip().lower())
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))


def build_dq_conditions(df, pk_cols, col_rules: dict):
    """
    Returns:
      bad_condition: Column(boolean)
      dq_reasons: list of (reason_code, condition_col, affected_cols_list)

    Supported checks (KEPT v2 + add):
      - not_null, length>0, range[a,b], ge0, gt0, parseable_ts, accepted{...}
      - null_policy modes: UNKNOWN_NOT_ALLOWED, EXCLUDE_ROW_IF_NULL
    """
    dq_reasons = []

    # PK not null (KEPT v2)
    for pk in pk_cols:
        if pk in df.columns:
            dq_reasons.append(("PK_NULL", F.col(pk).isNull(), [pk]))

    for col, rr in (col_rules or {}).items():
        if col not in df.columns:
            continue

        dq_checks = (rr or {}).get("dq_checks") or []
        null_pol = (rr or {}).get("null_policy") or {}
        null_mode = (null_pol.get("mode") or "ALLOW_NULL").upper()

        for chk in dq_checks:
            # accepted-values check (KEPT v2)
            if isinstance(chk, dict) and "accepted" in chk:
                allowed = [str(x).lower() for x in (chk.get("accepted") or [])]
                dq_reasons.append((
                    "ACCEPTED_FAIL",
                    (F.col(col).isNotNull()) & (~F.lower(F.col(col).cast("string")).isin(allowed)),
                    [col]
                ))
                continue

            c = str(chk).strip().lower()

            if c == "not_null":
                dq_reasons.append(("NOT_NULL_FAIL", F.col(col).isNull(), [col]))

            elif c == "length>0":
                dq_reasons.append(("LEN0_FAIL", (F.col(col).isNull()) | (F.length(F.col(col).cast("string")) == 0), [col]))

            elif c.startswith("range["):
                rg = parse_range_token(c)
                if rg:
                    lo, hi = rg
                    raw = F.col(col)
                    v = raw.cast("double")

                    # ==============================
                    # FIX (ADD): range should fail if:
                    #   - parsed numeric is out of range
                    #   - OR raw is non-null but cannot be parsed (cast -> null)
                    # This is the key difference that makes v3 show bad=0.
                    # ==============================
                    parse_fail = raw.isNotNull() & v.isNull()
                    range_fail = v.isNotNull() & ((v < F.lit(lo)) | (v > F.lit(hi)))
                    dq_reasons.append(("RANGE_FAIL", parse_fail | range_fail, [col]))

            elif c == "ge0":
                v = F.col(col).cast("double")
                dq_reasons.append(("GE0_FAIL", (v.isNotNull()) & (v < F.lit(0.0)), [col]))

            elif c == "gt0":
                v = F.col(col).cast("double")
                dq_reasons.append(("GT0_FAIL", (v.isNotNull()) & (v <= F.lit(0.0)), [col]))

            elif c == "parseable_ts":
                s = F.col(col).cast("string")
                ok = s.rlike(r"^\{?\d{4}-\d{2}-\d{2}.*")
                dq_reasons.append(("PARSEABLE_TS_FAIL", (s.isNotNull()) & (~ok), [col]))

            elif c == "unique_per_version":
                # informational; actual dedup is handled by dedup_latest()
                pass

        # UNKNOWN not allowed (KEPT v2)
        if null_mode == "UNKNOWN_NOT_ALLOWED":
            dq_reasons.append(("UNKNOWN_NOT_ALLOWED", F.lower(F.col(col).cast("string")) == F.lit("unknown"), [col]))

        # Exclude row if null (KEPT v2)
        if null_mode == "EXCLUDE_ROW_IF_NULL":
            dq_reasons.append(("EXCLUDE_IF_NULL", F.col(col).isNull(), [col]))

    if not dq_reasons:
        return F.lit(False), []

    bad_condition = None
    for _, cond, _ in dq_reasons:
        bad_condition = cond if bad_condition is None else (bad_condition | cond)

    return bad_condition, dq_reasons


# -----------------------------
# FIXED: add_dq_columns without array_remove(None) (KEPT v2)
# -----------------------------
def add_dq_columns(df, dq_reasons):
    reason_exprs = [F.when(cond, F.lit(code)).otherwise(F.lit(None)) for code, cond, _ in dq_reasons]
    cols_exprs = [F.when(cond, F.lit(",".join(cols))).otherwise(F.lit(None)) for _, cond, cols in dq_reasons]

    df = df.withColumn("_dq_reason_arr", F.array(*reason_exprs))
    df = df.withColumn("_dq_cols_arr", F.array(*cols_exprs))

    df = df.withColumn("_dq_reason_arr", F.expr("filter(_dq_reason_arr, x -> x is not null)"))
    df = df.withColumn("_dq_cols_arr", F.expr("filter(_dq_cols_arr, x -> x is not null)"))

    return (df
            .withColumn("dq_reason", F.concat_ws("|", F.col("_dq_reason_arr")))
            .withColumn("dq_columns", F.concat_ws("|", F.col("_dq_cols_arr")))
            .drop("_dq_reason_arr", "_dq_cols_arr"))


# -----------------------------
# Dedup (unique per version) (KEPT v2)
# -----------------------------
def dedup_latest(df, pk_cols):
    order_cols = []
    if "__commit_ts" in df.columns:
        order_cols.append(F.col("__commit_ts").desc())
    if "__lsn_num" in df.columns:
        order_cols.append(F.col("__lsn_num").desc())
    if "updated_at" in df.columns:
        order_cols.append(F.col("updated_at").desc())

    if not order_cols:
        return df

    existing_pk = [c for c in pk_cols if c in df.columns]
    if not existing_pk:
        return df

    w = Window.partitionBy(*[F.col(c) for c in existing_pk]).orderBy(*order_cols)
    return df.withColumn("__rn", F.row_number().over(w)).filter(F.col("__rn") == 1).drop("__rn")


# -----------------------------
# ADDED (optional but recommended): Align bad_df to existing quarantine table schema
# to avoid schema mismatch errors on append.
# -----------------------------
def align_df_to_bq_table_schema(spark, df, table_fqn: str):
    try:
        existing = spark.read.format("bigquery").option("table", table_fqn).load()
        target_schema = existing.schema
    except Exception as e:
        print(f"[INFO] Quarantine table not found/cannot read schema ({table_fqn}). Will create new. Details: {e}")
        return df, False

    df_cols = set(df.columns)
    select_exprs = []
    for f in target_schema.fields:
        name = f.name
        target_type = f.dataType
        if name in df_cols:
            select_exprs.append(F.col(name).cast(target_type).alias(name))
        else:
            select_exprs.append(F.lit(None).cast(target_type).alias(name))

    return df.select(*select_exprs), True


# -----------------------------
# ADDED: Robust rule lookup by multiple aliases
# -----------------------------
def get_rule_tbl(rules_by_table: dict, ct: str):
    """
    ct example: "public.clinics"
    possible keys in JSON:
      - "clinics"
      - "public.clinics"
      - "public_clinics"
    """
    src_tbl_short = ct.split(".", 1)[1] if "." in ct else ct              # "clinics"
    bq_table_name = contract_table_to_bq_name(ct)                         # "public_clinics"
    bq_no_public = bq_table_name.replace("public_", "", 1) if bq_table_name.startswith("public_") else bq_table_name

    return (
        rules_by_table.get(src_tbl_short) or
        rules_by_table.get(bq_no_public) or
        rules_by_table.get(bq_table_name) or
        rules_by_table.get(ct) or
        {}
    )


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--silver_dataset", required=True)
    parser.add_argument("--curated_dataset", required=True)
    parser.add_argument("--quarantine_dataset", default="silver_curated_quarantine")
    parser.add_argument("--dq_dataset", default="silver_curated_dq")
    parser.add_argument("--contract_path", required=True)
    parser.add_argument("--rules_json_path", required=True)
    parser.add_argument("--temp_gcs_bucket", required=True)
    parser.add_argument("--tables", default="ALL")
    parser.add_argument("--exclude_deleted", default="true")
    args = parser.parse_args()

    contract = load_contract(args.contract_path)
    global_cfg = contract.get("global", {})
    timezone_str = global_cfg.get("timezone", "Asia/Ho_Chi_Minh")
    contract_tables = contract.get("tables", {})

    rules_by_table = load_rules_json(args.rules_json_path)

    spark = (SparkSession.builder
             .appName("silver_to_silver_curated_dq_v2_2_1")
             .config("spark.sql.session.timeZone", timezone_str)
             .config("temporaryGcsBucket", args.temp_gcs_bucket)
             .getOrCreate())

    if args.tables.strip().upper() == "ALL":
        selected = list(contract_tables.keys())
    else:
        selected = [t.strip() for t in args.tables.split(",") if t.strip()]

    exclude_deleted = str(args.exclude_deleted).strip().lower() == "true"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    metrics_rows = []

    for ct in selected:
        if ct not in contract_tables:
            print(f"[WARN] Skip unknown contract table: {ct}")
            continue

        pk_cols = contract_tables[ct].get("primary_key") or global_cfg.get("primary_key_default") or ["id"]

        bq_table = contract_table_to_bq_name(ct)
        src = f"{args.project}.{args.silver_dataset}.{bq_table}"
        dst = f"{args.project}.{args.curated_dataset}.{bq_table}"
        q_dst = f"{args.project}.{args.quarantine_dataset}.{bq_table}__quarantine"

        print(f"\n=== Processing {ct} ===")
        print(f"READ : {src}")
        print(f"WRITE: {dst}")
        print(f"BAD  : {q_dst}")

        df = spark.read.format("bigquery").option("table", src).load()

        # Soft delete canonical handling (KEPT v2)
        if exclude_deleted and "is_deleted" in df.columns:
            df = df.filter((F.col("is_deleted").isNull()) | (F.col("is_deleted") == F.lit(False)))

        # ==============================
        # CHANGED: Load rules using robust alias lookup
        # ==============================
        rule_tbl = get_rule_tbl(rules_by_table, ct)
        col_rules = (rule_tbl.get("columns") or {})

        # Support both old/new rule JSON fields
        filters_desc = rule_tbl.get("filters_desc") or rule_tbl.get("filters") or []
        filters_sql = rule_tbl.get("filters_sql") or []

        if not rule_tbl:
            # print(f"[WARN] No rules found for table: {ct} (aliases: {ct.split('.',1)[1]} | {bq_table} | {ct})")
            # PATCH: avoid IndexError when ct has no dot (e.g., "public_booking_events")
            alias1 = ct.split(".", 1)[1] if "." in ct else ct
            print(f"[WARN] No rules found for table: {ct} (aliases: {alias1} | {bq_table} | {ct})")

        else:
            print(f"[INFO] Rules loaded for {ct}: cols={len(col_rules)} filters_desc={len(filters_desc)} filters_sql={len(filters_sql)}")

        # Keep v2 behavior: DO NOT apply prose filters (log only)
        if filters_desc:
            print(f"[INFO] Skip descriptive filters for {ct} (not SQL): {filters_desc}")

        # (Optional) apply filters_sql only if you later put real SQL here
        # if filters_sql:
        #     for expr_s in filters_sql:
        #         try:
        #             df = df.filter(str(expr_s))
        #         except Exception as e:
        #             print(f"[WARN] Failed to apply filters_sql '{expr_s}' for {ct}: {e}")

        # Normalize strings globally (KEPT v2)
        df = normalize_string_columns(df)

        # Dedup latest version by PK (KEPT v2)
        df = dedup_latest(df, pk_cols)

        # Apply null fill policies (KEPT v2)
        df = apply_null_policies(df, col_rules)

        # Build DQ condition and split good/bad
        bad_cond, dq_reasons = build_dq_conditions(df, pk_cols, col_rules)

        bad_df = df.filter(bad_cond) if dq_reasons else df.limit(0)
        good_df = df.filter(~bad_cond) if dq_reasons else df

        # Add dq columns for bad records
        if dq_reasons:
            bad_df = add_dq_columns(bad_df, dq_reasons)
        else:
            bad_df = bad_df.withColumn("dq_reason", F.lit(None).cast("string")).withColumn("dq_columns", F.lit(None).cast("string"))

        # Audit columns
        good_df = (good_df
                   .withColumn("__dq_run_id", F.lit(run_id))
                   .withColumn("__dq_loaded_at", F.current_timestamp()))
        bad_df = (bad_df
                  .withColumn("__dq_run_id", F.lit(run_id))
                  .withColumn("__dq_loaded_at", F.current_timestamp())
                  .withColumn("__dq_source_table", F.lit(ct)))

        # OPTIMIZATION: persist + repartition (KEPT v2)
        good_df = good_df.persist()
        bad_df = bad_df.persist()

        good_df = good_df.repartition(16)
        bad_df = bad_df.repartition(8)

        # Write curated (overwrite)
        (good_df.write.format("bigquery")
         .option("table", dst)
         .mode("overwrite")
         .save())

        # ==============================
        # ADDED: Align schema before append to quarantine (prevents schema mismatch)
        # ==============================
        aligned_bad_df, q_exists = align_df_to_bq_table_schema(spark, bad_df, q_dst)
        write_mode = "append" if q_exists else "overwrite"

        (aligned_bad_df.write.format("bigquery")
         .option("table", q_dst)
         .mode(write_mode)
         .save())

        # Metrics: 1-pass collection (KEPT v2)
        metrics = (
            good_df.select(F.lit("good").alias("type"))
            .unionByName(bad_df.select(F.lit("bad").alias("type")))
            .groupBy("type")
            .count()
            .collect()
        )
        total = sum(r["count"] for r in metrics)
        good = next((r["count"] for r in metrics if r["type"] == "good"), 0)
        bad = next((r["count"] for r in metrics if r["type"] == "bad"), 0)

        metrics_rows.append((run_id, ct, total, good, bad, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")))
        print(f"[OK] {ct} total={total} good={good} bad={bad}")

        good_df.unpersist()
        bad_df.unpersist()

    # Write metrics table (KEPT v2)
    if metrics_rows:
        metrics_schema = T.StructType([
            T.StructField("dq_run_id", T.StringType(), False),
            T.StructField("table_name", T.StringType(), False),
            T.StructField("total_rows", T.LongType(), False),
            T.StructField("good_rows", T.LongType(), False),
            T.StructField("bad_rows", T.LongType(), False),
            T.StructField("run_ts_utc", T.StringType(), False),
        ])
        mdf = spark.createDataFrame(metrics_rows, schema=metrics_schema)
        m_tbl = f"{args.project}:{args.dq_dataset}.dq_run_metrics"
        (mdf.write.format("bigquery")
         .option("table", m_tbl)
         .mode("append")
         .save())

    spark.stop()


if __name__ == "__main__":
    main()
