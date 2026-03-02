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
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Try multiple providers to survive connector/classpath differences across clusters.
BQ_DATA_SOURCES = [
    "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider",
    "com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider",
    "com.google.cloud.spark.bigquery",
    "bigquery",
]

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
    raise RuntimeError(
        "Cannot read BigQuery table. Tried providers: " + " | ".join(errors)
    )


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
    raise RuntimeError(
        "Cannot write BigQuery table. Tried providers: " + " | ".join(errors)
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


def _clean_string(col):
    return F.trim(F.coalesce(col.cast("string"), F.lit("")))


def _digits_only(col):
    return F.regexp_replace(col, r"\D", "")


def _strip_vietnamese_accents(col):
    out = F.lower(col)
    replacements = [
        (r"[àáạảãăắằẳẵặâấầẩẫậ]", "a"),
        (r"[èéẹẻẽêếềểễệ]", "e"),
        (r"[ìíịỉĩ]", "i"),
        (r"[òóọỏõôốồổỗộơớờởỡợ]", "o"),
        (r"[ùúụủũưứừửữự]", "u"),
        (r"[ỳýỵỷỹ]", "y"),
        (r"đ", "d"),
    ]
    for pattern, replacement in replacements:
        out = F.regexp_replace(out, pattern, replacement)
    return out


def build_master_id_expr(name_col, dob_col, phone_col):
    n = _strip_vietnamese_accents(_clean_string(name_col))
    n = F.regexp_replace(n, r"[{}]", "")
    n = F.regexp_replace(n, r"\s+", " ")
    n = F.trim(n)
    n = F.when((n == "") | (n == "nan"), F.lit("")).otherwise(n)

    last_word = F.regexp_extract(n, r"(\S+)$", 1)
    leading_words = F.regexp_extract(n, r"^(.*)\s+\S+$", 1)
    initials = F.regexp_replace(leading_words, r"(^|\s+)(\S)\S*", "$2")
    name_token = F.when(n == "", F.lit("")).otherwise(F.concat(last_word, initials))

    dob_raw = _clean_string(dob_col)
    dob_raw = F.when((dob_raw == "") | (F.lower(dob_raw) == "nan"), F.lit("")).otherwise(dob_raw)
    dob_date = F.when(dob_raw == "", F.lit("")).otherwise(F.split(dob_raw, r"\s+").getItem(0))
    dob_token = _digits_only(F.regexp_replace(dob_date, r"[{}]", ""))

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
        build_master_id_expr(
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
        build_master_id_expr(
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

    spark.stop()


if __name__ == "__main__":
    main()
