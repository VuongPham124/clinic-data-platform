# -*- coding: utf-8 -*-
"""
Patched (Dataproc-ready) version of your master_patient_id generator.

What changed vs your original notebook script:
- Keep the SAME normalization rules / logic (format_patient_name, format_dob_yyyymmdd, format_phone_number, build_master_id)
- Only replace CSV IO with BigQuery IO (Spark BigQuery connector)
- Write outputs to BigQuery tables for easy verification
- No CDC merge / watermark logic yet (you said you'll handle merge later)

Default flow:
1) Read 4 tables from BigQuery (bq_stage):
   - public_clinic_users_cdc
   - public_clinic_patients_cdc
   - public_users_cdc
   - public_patients_cdc
2) Join:
   - clinic_patients.user_id = clinic_users.id
   - patients.user_id = users.id
3) Generate master_patient_id:
   - master_patient_id = formatted_name + '_' + dob_yyyymmdd + '_' + phone (same formatting logic)
4) Write:
   - master.patients_master_patient_id_v1
   - master.clinic_patients_master_patient_id_v1

Run (Dataproc):
gcloud dataproc jobs submit pyspark gen_master_patient_id_script_patched.py \
  --cluster=<CLUSTER> --region=<REGION> \
  --jars=gs://.../spark-bigquery-with-dependencies_2.12-0.xx.x.jar \
  -- \
  --temp-gcs-bucket=<TEMP_BUCKET>
"""

from __future__ import annotations

import argparse
import unicodedata

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def _bq_provider_for_spark(spark: SparkSession) -> str:
    version = getattr(spark, "version", "") or ""
    if version.startswith("3.5"):
        return "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider"
    if version.startswith("3.4"):
        return "com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider"
    # Fallback for other runtimes.
    return "com.google.cloud.spark.bigquery"


# =====================
# 1) SAME RULES as your script
# =====================
def format_patient_name(name: object) -> str:
    """Return a short deterministic token from full name: last_word + initials.

    Example: "Nguyễn Văn A" -> "anv" (accentless, lowercase)
    """
    if name is None:
        return ""
    s = str(name).strip()
    if not s or s.lower() == "nan":
        return ""

    # Remove accents
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("utf-8")

    words = s.split()
    if not words:
        return ""

    last_word = words[-1].lower()
    initials = "".join(w[0].lower() for w in words[:-1] if w)
    return last_word + initials


import re

def clean_braces(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"[{}]", "", str(s)).strip()

def digits_only(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\D", "", str(s))


def format_dob_yyyymmdd(dob: object) -> str:
    """Take a DOB string or timestamp-like value and return YYYYMMDD."""
    if dob is None:
        return ""
    s = str(dob).strip()
    if not s or s.lower() == "nan":
        return ""

    date_part = s.split(" ")[0]  # keep only date part if it contains time
    return date_part.replace("-", "")


# def format_phone_number(phone: object) -> str:
#     """Normalize phone numbers that sometimes look like floats (e.g., 939999888.0)."""
#     if phone is None:
#         return ""
#     s = str(phone).strip()
#     if not s or s.lower() == "nan":
#         return ""

#     try:
#         f = float(s)
#         if f == int(f):
#             return str(int(f))
#         return str(f)
#     except ValueError:
#         return s
def format_phone_number(phone: object) -> str:
    """
    Preserve leading zero.
    Do NOT cast to float/int.
    Only keep digits.
    """
    if phone is None:
        return ""

    s = str(phone).strip()
    if not s or s.lower() == "nan":
        return ""

    # remove decimal part if exists (e.g. 939999888.0)
    if "." in s:
        s = s.split(".")[0]

    # keep digits only
    s = re.sub(r"\D", "", s)

    return s


def build_master_id(name: object, dob: object, phone: object) -> str:
    a = format_patient_name(name)
    b = format_dob_yyyymmdd(dob)
    c = format_phone_number(phone)

    a = clean_braces(a)

    b= digits_only(clean_braces(b))
    c = digits_only(clean_braces(c))

    out = f"{a}_{b}_{c}"
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_")


# =====================
# 2) BigQuery IO helpers
# =====================
def read_bq(spark: SparkSession, table: str, temp_gcs_bucket: str):
    provider = _bq_provider_for_spark(spark)
    return (
        spark.read.format(provider)
        .option("table", table)
        .option("temporaryGcsBucket", temp_gcs_bucket)
        .load()
    )


def write_bq(df, table: str, temp_gcs_bucket: str, mode: str):
    provider = _bq_provider_for_spark(df.sparkSession)
    (
        df.write.format(provider)
        .option("table", table)
        .option("temporaryGcsBucket", temp_gcs_bucket)
        .mode(mode)
        .save()
    )


def pick_name_col(df):
    # Keep your original fallback logic: prefer full_name else name
    cols = set(df.columns)
    if "full_name" in cols:
        return F.col("full_name")
    if "name" in cols:
        return F.col("name")
    # if neither exists, return null
    return F.lit(None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--temp-gcs-bucket", required=True, help="GCS bucket for BigQuery connector temp data")
    ap.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"], help="Write mode for output tables")

    # Input tables (defaults for your clinic CDC staging)
    ap.add_argument("--clinic-users-table", default="wata-clinicdataplatform-gcp.silver_curated.public_clinic_users")
    ap.add_argument("--clinic-patients-table", default="wata-clinicdataplatform-gcp.silver_curated.public_clinic_patients")
    ap.add_argument("--users-table", default="wata-clinicdataplatform-gcp.silver_curated.public_users")
    ap.add_argument("--patients-table", default="wata-clinicdataplatform-gcp.silver_curated.public_patients")

    # Output tables
    ap.add_argument("--out-patients-table", default="wata-clinicdataplatform-gcp.master.patients_master_patient_id_v1")
    ap.add_argument("--out-clinic-patients-table", default="wata-clinicdataplatform-gcp.master.clinic_patients_master_patient_id_v1")

    args, _ = ap.parse_known_args()

    spark = (
        SparkSession.builder.appName("gen_master_patient_id_patched")
        # BigQuery connector config is driven by --jars and temporaryGcsBucket
        .getOrCreate()
    )

    # UDF: same rules
    master_id_udf = F.udf(build_master_id, StringType())

    # ---------------------
    # Load tables
    # ---------------------
    clinic_users_df = read_bq(spark, args.clinic_users_table, args.temp_gcs_bucket)
    clinic_patients_df = read_bq(spark, args.clinic_patients_table, args.temp_gcs_bucket)
    users_df = read_bq(spark, args.users_table, args.temp_gcs_bucket)
    patients_df = read_bq(spark, args.patients_table, args.temp_gcs_bucket)

    # ---------------------
    # Join Clinic data: clinic_patients -> clinic_users
    # (same keys as your script: left_on user_id, right_on id)
    # ---------------------
    clinic_users_sel = (
        clinic_users_df.select(
            F.col("id").alias("clinic_user_id"),
            F.col("date_of_birth").alias("clinic_user_dob"),
            F.col("phone_number").alias("clinic_user_phone_number"),
            pick_name_col(clinic_users_df).alias("clinic_user_full_name"),
        )
    )

    clinic_patients_enriched = (
        clinic_patients_df.alias("cp")
        .join(clinic_users_sel.alias("cu"), F.col("cp.user_id") == F.col("cu.clinic_user_id"), "left")
    )

    clinic_patients_enriched = clinic_patients_enriched.withColumn(
        "master_patient_id",
        master_id_udf(
            F.col("clinic_user_full_name"),
            F.col("clinic_user_dob"),
            F.col("clinic_user_phone_number"),
        ),
    )

    # ---------------------
    # Join General data: patients -> users
    # ---------------------
    users_sel = (
        users_df.select(
            F.col("id").alias("user_id_right"),
            F.col("date_of_birth").alias("user_dob"),
            F.col("phone_number").alias("user_phone_number"),
            pick_name_col(users_df).alias("user_full_name"),
        )
    )

    patients_enriched = (
        patients_df.alias("p")
        .join(users_sel.alias("u"), F.col("p.user_id") == F.col("u.user_id_right"), "left")
    )

    patients_enriched = patients_enriched.withColumn(
        "master_patient_id",
        master_id_udf(
            F.col("user_full_name"),
            F.col("user_dob"),
            F.col("user_phone_number"),
        ),
    )

    # ---------------------
    # Write outputs
    # ---------------------
    write_bq(patients_enriched, args.out_patients_table, args.temp_gcs_bucket, args.write_mode)
    write_bq(clinic_patients_enriched, args.out_clinic_patients_table, args.temp_gcs_bucket, args.write_mode)

    # Print small samples to driver logs
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
