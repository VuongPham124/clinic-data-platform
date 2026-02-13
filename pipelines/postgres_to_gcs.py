import os
import io
import csv
import gzip
import json
import logging
import argparse
import psycopg2
from datetime import datetime, timezone
from google.cloud import storage
from airflow.hooks.base import BaseHook

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("pg_to_gcs_raw_snapshot_copy")

# =====================================================
# PIPELINE IDENTITY
# =====================================================
PIPELINE = "pg_to_gcs_raw_snapshot"
SOURCE = "snapshot"

# =====================================================
# POSTGRES CONFIG
# =====================================================
conn = BaseHook.get_connection("postgres_source")

PG_CONN = {
    "host": conn.host,
    "port": conn.port,
    "dbname": conn.schema,
    "user": conn.login,
    "password": conn.password,
}

SCHEMA = "public"
ORDER_COLUMN = "id"

# logical chunk size (ROWS)
DEFAULT_CHUNK_SIZE = int(os.getenv("COPY_CHUNK_SIZE", "5000000"))

# =====================================================
# GCS CONFIG
# =====================================================
RAW_BUCKET = "amaz-raw"
storage_client = storage.Client()
raw_bucket = storage_client.bucket(RAW_BUCKET)

# =====================================================
# TABLE LIST
# =====================================================
TABLES = [
    "booking_events",
    "booking_feedbacks",
    "bookings",
    "clinic_booking_events",
    "clinic_booking_examinations",
    "clinic_booking_feedbacks",
    "clinic_bookings",
    "clinic_doctors",
    "clinic_examination_templates",
    "clinic_major_associates",
    "clinic_majors",
    "clinic_partner_accounts",
    "clinic_patients",
    "clinic_ratings",
    "clinic_rooms",
    "clinic_services",
    "clinic_shifts",
    "clinic_users",
    "clinics",
    "doctor_majors",
    "doctor_working_schedules",
    "doctor_working_slots",
    "doctors",
    "drug_details",
    "health_records",
    "icd10_diagnoses",
    "inventory_deliveries",
    "medical_histories",
    "medicine_import_details",
    "medicine_imports",
    "medicines",
    "metrics",
    "notification_logs",
    "outside_prescription_medicines",
    "patients",
    "prescription_medicine_details",
    "prescription_medicines",
    "prescriptions",
    "ratings",
    "subclinical_results",
    "transactions",
    "user_voucher_associates",
    "users",
    "vital_signs",
    "vouchers",
    "wallet_partners",
    "wallets",
]

# =====================================================
# SAFE SELECT (timestamp -> string UTC)
# =====================================================
def build_safe_select(cur, table):
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
        """,
        (SCHEMA, table)
    )

    exprs = []

    for col, dtype in cur.fetchall():
        qc = f'"{col}"'
        dtype = dtype.lower()

        # -------- TIMESTAMP --------
        if "timestamp" in dtype:
            exprs.append(
                f"to_char({qc} AT TIME ZONE 'UTC', "
                f"'YYYY-MM-DD HH24:MI:SS.US') AS {qc}"
            )

        # -------- JSON / JSONB --------
        elif dtype in ("json", "jsonb"):
            exprs.append(f"{qc}::text AS {qc}")

        # -------- ARRAY (nếu có) --------
        elif "array" in dtype:
            exprs.append(f"{qc}::text AS {qc}")

        # -------- NUMERIC / DECIMAL --------
        elif dtype in ("numeric", "decimal"):
            exprs.append(f"{qc}::text AS {qc}")

        # -------- DEFAULT --------
        else:
            exprs.append(qc)

    return exprs

# =====================================================
# INGEST TABLE (COPY + LOGICAL CHUNK)
# =====================================================
def ingest_table(table, load_date, run_id, chunk_size):
    logger.info(f"Start ingest table={SCHEMA}.{table} (COPY + CHUNK)")

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    written_rows = 0
    part = 0

    try:
        # -------- detect id range --------
        cur.execute(
            f"SELECT MIN({ORDER_COLUMN}), MAX({ORDER_COLUMN}) "
            f"FROM {SCHEMA}.{table}"
        )
        min_id, max_id = cur.fetchone()

        if min_id is None or max_id is None:
            logger.warning(f"Table {table} is empty")
            return {
                "table": f"{SCHEMA}.{table}",
                "status": "SUCCESS",
                "written_rows": 0
            }

        logger.info(
            f"Table {table} id_range=[{min_id}, {max_id}] "
            f"chunk_size={chunk_size}"
        )

        select_exprs = build_safe_select(cur, table)
        select_sql = ", ".join(select_exprs)

        start_id = min_id

        while start_id <= max_id:
            end_id = start_id + chunk_size - 1

            copy_sql = f"""
                COPY (
                    SELECT {select_sql}
                    FROM {SCHEMA}.{table}
                    WHERE "{ORDER_COLUMN}" BETWEEN {start_id} AND {end_id}
                )
                TO STDOUT
                WITH (
                    FORMAT CSV,
                    HEADER TRUE,
                    ENCODING 'UTF8',
                    QUOTE '"',
                    ESCAPE '"',
                    FORCE_QUOTE *
                )
            """

            raw_path = (
                f"{SOURCE}/"
                f"{SCHEMA}.{table}/"
                f"load_date={load_date}/run_id={run_id}/"
                f"part-{part:05d}.csv.gz"
            )

            blob = raw_bucket.blob(raw_path)
            buf = io.BytesIO()

            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                with io.TextIOWrapper(gz, encoding="utf-8", newline="") as txt:
                    cur.copy_expert(copy_sql, txt)

            buf.seek(0)
            blob.upload_from_file(
                buf,
                content_type="application/gzip"
            )

            written_rows += chunk_size
            logger.info(
                f"Written table={table} part={part} "
                f"id_range=[{start_id}, {end_id}]"
            )

            start_id = end_id + 1
            part += 1

        logger.info(
            f"Finished table={table} parts={part}"
        )

        return {
            "table": f"{SCHEMA}.{table}",
            "status": "SUCCESS",
            "parts": part
        }

    except Exception as e:
        logger.exception(
            f"FAILED ingest table={SCHEMA}.{table} error={str(e)}"
        )
        return {
            "table": f"{SCHEMA}.{table}",
            "status": "FAILED",
            "error": str(e)
        }

    finally:
        cur.close()
        conn.close()

# =====================================================
# ARGPARSE
# =====================================================
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--load-date", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE
    )
    return p.parse_args()

# =====================================================
# MAIN
# =====================================================
def run(
    load_date: str,
    run_id: str,
    chunk_size: int = 5_000_000,
):
    started_at = datetime.now(timezone.utc).isoformat()

    table_results = []
    success_tables = 0
    failed_tables = 0

    for table in TABLES:
        result = ingest_table(
            table=table,
            load_date=load_date,
            run_id=run_id,
            chunk_size=chunk_size,
        )
        table_results.append(result)

        if result["status"] == "SUCCESS":
            success_tables += 1
        else:
            failed_tables += 1

    finished_at = datetime.now(timezone.utc).isoformat()

    # =================================================
    # SOURCE-LEVEL HISTORY
    # =================================================
    history_record = {
        "source": SOURCE,
        "pipeline": PIPELINE,
        "load_date": load_date,
        "run_id": run_id,
        "chunk_size": chunk_size,
        "status": "SUCCESS" if failed_tables == 0 else "PARTIAL_SUCCESS",
        "started_at": started_at,
        "finished_at": finished_at,
        "tables_total": len(TABLES),
        "tables_success": success_tables,
        "tables_failed": failed_tables,
    }

    history_path = (
        f"{SOURCE}/_HISTORY/"
        f"load_date={load_date}/run_id={run_id}/summary.json"
    )

    raw_bucket.blob(history_path).upload_from_string(
        json.dumps(history_record, indent=2),
        content_type="application/json"
    )

    logger.info(
        f"Ingest snapshot finished | "
        f"tables_total={len(TABLES)} "
        f"success={success_tables} "
        f"failed={failed_tables}"
    )
