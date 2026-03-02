# -*- coding: utf-8 -*-
"""
Dataproc-ready master_patient_id generator optimized for Spark execution.

Key improvements:
- Replace Python UDF with Spark SQL expressions to avoid Python row-by-row overhead.
- Broadcast right-side user tables for join efficiency.
"""

from __future__ import annotations

import argparse
import re
import unicodedata

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BQ_DATA_SOURCES = [
    "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider",
    "com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider",
    "com.google.cloud.spark.bigquery",
    "bigquery",
]

# Legacy utilities kept for traceability with old script.
# Main execution path now uses build_master_id_expr() for Spark-native performance.
def clean_braces(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"[{}]", "", str(s)).strip()


def format_patient_name(name: object) -> str:
    """Legacy behavior: last_word + initials (accentless, lowercase)."""
    if name is None:
        return ""
    s = str(name).strip()
    if not s or s.lower() == "nan":
        return ""

    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("utf-8")
    words = s.split()
    if not words:
        return ""

    last_word = words[-1].lower()
    initials = "".join(w[0].lower() for w in words[:-1] if w)
    return last_word + initials


def format_dob_yyyymmdd(dob: object) -> str:
    """Legacy behavior: keep date part then strip '-'."""
    if dob is None:
        return ""
    s = str(dob).strip()
    if not s or s.lower() == "nan":
        return ""
    date_part = s.split(" ")[0]
    return date_part.replace("-", "")


def format_phone_number(phone: object) -> str:
    """Legacy behavior: keep left side of decimal point and digits only."""
    if phone is None:
        return ""
    s = str(phone).strip()
    if not s or s.lower() == "nan":
        return ""
    if "." in s:
        s = s.split(".")[0]
    return re.sub(r"\D", "", s)


def read_bq(spark: SparkSession, table: str, temp_gcs_bucket: str):
    errors = []
    for source in BQ_DATA_SOURCES:
        try:
            return (
                spark.read.format(source)
                .option("table", table)
                .option("temporaryGcsBucket", temp_gcs_bucket)
                .load()
            )
        except Exception as exc:
            errors.append(f"{source}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot read BigQuery table. Tried providers: " + " | ".join(errors))


def write_bq(df, table: str, temp_gcs_bucket: str, mode: str):
    errors = []
    for source in BQ_DATA_SOURCES:
        try:
            (
                df.write.format(source)
                .option("table", table)
                .option("temporaryGcsBucket", temp_gcs_bucket)
                .mode(mode)
                .save()
            )
            return
        except Exception as exc:
            errors.append(f"{source}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot write BigQuery table. Tried providers: " + " | ".join(errors))


def pick_name_col(df):
    cols = set(df.columns)
    if "full_name" in cols:
        return F.col("full_name")
    if "name" in cols:
        return F.col("name")
    return F.lit(None)


def _clean_string(col):
    return F.trim(F.coalesce(col.cast("string"), F.lit("")))


def _digits_only(col):
    return F.regexp_replace(col, r"\D", "")


def build_master_id_expr(name_col, dob_col, phone_col):
    # name token: last_word + initials of preceding words
    n = F.lower(_clean_string(name_col))
    n = F.regexp_replace(n, r"[{}]", "")
    n = F.regexp_replace(n, r"\s+", " ")
    n = F.trim(n)
    n = F.when((n == "") | (n == "nan"), F.lit("")).otherwise(n)

    last_word = F.regexp_extract(n, r"(\S+)$", 1)
    leading_words = F.regexp_extract(n, r"^(.*)\s+\S+$", 1)
    initials = F.regexp_replace(leading_words, r"(^|\s+)(\S)\S*", "$2")
    name_token = F.when(n == "", F.lit("")).otherwise(F.concat(last_word, initials))

    # dob token: split date part then keep only digits
    dob_raw = _clean_string(dob_col)
    dob_raw = F.when((dob_raw == "") | (F.lower(dob_raw) == "nan"), F.lit("")).otherwise(dob_raw)
    dob_date = F.when(dob_raw == "", F.lit("")).otherwise(F.split(dob_raw, r"\s+").getItem(0))
    dob_token = _digits_only(F.regexp_replace(dob_date, r"[{}]", ""))

    # phone token: keep left side of decimal point then digits only
    phone_raw = _clean_string(phone_col)
    phone_raw = F.when((phone_raw == "") | (F.lower(phone_raw) == "nan"), F.lit("")).otherwise(phone_raw)
    phone_left = F.when(phone_raw == "", F.lit("")).otherwise(F.regexp_extract(phone_raw, r"^([^.]*)", 1))
    phone_token = _digits_only(F.regexp_replace(phone_left, r"[{}]", ""))

    return F.concat_ws(
        "_",
        F.when(name_token != "", name_token),
        F.when(dob_token != "", dob_token),
        F.when(phone_token != "", phone_token),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--temp-gcs-bucket", required=True, help="GCS bucket for BigQuery connector temp data")
    ap.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"], help="Write mode for output tables")

    ap.add_argument("--clinic-users-table", default="wata-clinicdataplatform-gcp.silver_curated.public_clinic_users")
    ap.add_argument("--clinic-patients-table", default="wata-clinicdataplatform-gcp.silver_curated.public_clinic_patients")
    ap.add_argument("--users-table", default="wata-clinicdataplatform-gcp.silver_curated.public_users")
    ap.add_argument("--patients-table", default="wata-clinicdataplatform-gcp.silver_curated.public_patients")

    ap.add_argument("--out-patients-table", default="wata-clinicdataplatform-gcp.master.patients_master_patient_id_v1")
    ap.add_argument("--out-clinic-patients-table", default="wata-clinicdataplatform-gcp.master.clinic_patients_master_patient_id_v1")

    args, _ = ap.parse_known_args()

    spark = (
        SparkSession.builder.appName("gen_master_patient_id_patched")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    clinic_users_df = read_bq(spark, args.clinic_users_table, args.temp_gcs_bucket)
    clinic_patients_df = read_bq(spark, args.clinic_patients_table, args.temp_gcs_bucket)
    users_df = read_bq(spark, args.users_table, args.temp_gcs_bucket)
    patients_df = read_bq(spark, args.patients_table, args.temp_gcs_bucket)

    clinic_users_sel = clinic_users_df.select(
        F.col("id").alias("clinic_user_id"),
        F.col("date_of_birth").alias("clinic_user_dob"),
        F.col("phone_number").alias("clinic_user_phone_number"),
        pick_name_col(clinic_users_df).alias("clinic_user_full_name"),
    )

    clinic_patients_enriched = (
        clinic_patients_df.alias("cp")
        .join(F.broadcast(clinic_users_sel).alias("cu"), F.col("cp.user_id") == F.col("cu.clinic_user_id"), "left")
        .withColumn(
            "master_patient_id",
            build_master_id_expr(
                F.col("clinic_user_full_name"),
                F.col("clinic_user_dob"),
                F.col("clinic_user_phone_number"),
            ),
        )
    )

    users_sel = users_df.select(
        F.col("id").alias("user_id_right"),
        F.col("date_of_birth").alias("user_dob"),
        F.col("phone_number").alias("user_phone_number"),
        pick_name_col(users_df).alias("user_full_name"),
    )

    patients_enriched = (
        patients_df.alias("p")
        .join(F.broadcast(users_sel).alias("u"), F.col("p.user_id") == F.col("u.user_id_right"), "left")
        .withColumn(
            "master_patient_id",
            build_master_id_expr(
                F.col("user_full_name"),
                F.col("user_dob"),
                F.col("user_phone_number"),
            ),
        )
    )

    write_bq(patients_enriched, args.out_patients_table, args.temp_gcs_bucket, args.write_mode)
    write_bq(clinic_patients_enriched, args.out_clinic_patients_table, args.temp_gcs_bucket, args.write_mode)

    print("\n[INFO] Sample patients_enriched:")
    patients_enriched.select(
        *[c for c in ["id", "user_id", "user_full_name", "user_dob", "user_phone_number", "master_patient_id"] if c in patients_enriched.columns]
    ).show(10, truncate=False)

    print("\n[INFO] Sample clinic_patients_enriched:")
    clinic_patients_enriched.select(
        *[c for c in ["id", "user_id", "clinic_user_full_name", "clinic_user_dob", "clinic_user_phone_number", "master_patient_id"] if c in clinic_patients_enriched.columns]
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
