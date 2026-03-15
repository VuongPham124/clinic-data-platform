import base64
import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, request
from unidecode import unidecode

from google.cloud import storage
from google.cloud import bigquery

app = Flask(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ["BQ_DATASET"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
STAGING_BUCKET = os.environ["STAGING_BUCKET"]
SILVER_DATASET = os.environ["BQ_SILVER_DATASET"]
MANUAL_FILE_PREFIX = os.environ["MANUAL_FILE_PREFIX"]
PIPELINE_CONFIG_TABLE = os.environ.get("PIPELINE_CONFIG_TABLE", "__pipeline_config")

storage_client = storage.Client()
bq_client = bigquery.Client(project=PROJECT_ID)


# ---- Pipeline config ----

def load_pipeline_steps(file_key: str) -> list[dict]:
    table_id = f"`{PROJECT_ID}.{DATASET}.{PIPELINE_CONFIG_TABLE}`"
    query = f"""
        SELECT step_order, target_table, query_template
        FROM {table_id}
        WHERE file_key = @file_key
          AND is_active = TRUE
        ORDER BY step_order ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_key", "STRING", file_key)
        ]
    )
    rows = bq_client.query(query, job_config=job_config).result()
    return [dict(row) for row in rows]


def run_pipeline_steps(file_key: str):
    steps = load_pipeline_steps(file_key)

    if not steps:
        print(f"No pipeline steps configured for: {file_key}")
        return

    print(f"Running {len(steps)} pipeline step(s) for: {file_key}")

    for step in steps:
        order = step["step_order"]
        target = step["target_table"]
        query = step["query_template"]

        query = (query
            .replace("{project}", PROJECT_ID)
            .replace("{dataset}", DATASET)
            .replace("{silver_dataset}", SILVER_DATASET)
        )

        print(f"  Step {order} -> {target} ...")
        job = bq_client.query(query.strip())
        result = job.result()
        affected = result.num_dml_affected_rows
        print(f"  Step {order} done. Rows affected: {affected if affected is not None else 'N/A'}")


# ---- Folder-based routing ----

def parse_folder_key(object_name: str) -> str | None:
    """
    Lấy file_key từ subfolder ngay dưới MANUAL_FILE_PREFIX.
    Ví dụ:
      manual_file/patients/patients1.xlsx  -> "patients"
      manual_file/inventory/inv.xlsx       -> "inventory"
      somefile.xlsx                        -> None (ignore)
      manual_file/file.xlsx                -> None (không có subfolder)
    """
    if not object_name.startswith(MANUAL_FILE_PREFIX):
        return None

    remainder = object_name[len(MANUAL_FILE_PREFIX):]
    parts = remainder.split("/")

    if len(parts) < 2 or parts[0] == "":
        return None

    subfolder = parts[0].strip().lower()
    subfolder = re.sub(r"[^a-z0-9_]+", "_", subfolder).strip("_")
    return subfolder or None


# ---- Column handling ----

def sanitize_bq_field(name: str) -> str:
    if name is None:
        name = ""
    name = str(name).strip()
    name = unidecode(name)
    name = re.sub(r"[^A-Za-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    if name == "":
        name = "col"
    if re.match(r"^\d", name):
        name = "_" + name
    return name[:300]


def sanitize_and_dedupe_columns(cols):
    seen = {}
    new_cols = []
    for c in cols:
        base = sanitize_bq_field("" if c is None else str(c))
        if base not in seen:
            seen[base] = 1
            final = base
        else:
            seen[base] += 1
            final = f"{base}_{seen[base]}"
        new_cols.append(final)
    return new_cols


# ---- Cloud Run handler ----

@app.post("/")
def handler():
    # 1. Parse event
    event = request.get_json(silent=True) or {}
    print(f"Received event: {event}")

    if "message" in event:
        msg = event.get("message", {})
        if "data" in msg:
            try:
                decoded_data = base64.b64decode(msg["data"]).decode("utf-8")
                event = json.loads(decoded_data)
            except Exception as e:
                print(f"Error decoding PubSub: {e}")

    # 2. Lấy bucket và file name
    bucket_name = event.get("bucket") or event.get("bucketId")
    object_name = event.get("name")

    if not bucket_name or not object_name:
        print(f"Invalid payload format: {event}")
        return ("Missing bucket or name", 400)

    # 3. Chỉ xử lý file từ RAW bucket
    if bucket_name != RAW_BUCKET:
        return (f"Ignored: Bucket {bucket_name} is not the RAW source", 204)

    lower = object_name.lower()
    if not (lower.endswith(".csv") or lower.endswith(".xlsx")):
        return ("Ignored (not csv/xlsx)", 204)

    # 4. Folder-based routing: chỉ xử lý file trong MANUAL_FILE_PREFIX/<subfolder>/
    file_key = parse_folder_key(object_name)
    if file_key is None:
        print(f"Ignored: '{object_name}' is not under {MANUAL_FILE_PREFIX}<subfolder>/")
        return (f"Ignored: file not under {MANUAL_FILE_PREFIX}<subfolder>/", 204)

    print(f"file_key={file_key}, object={object_name}")

    file_name = object_name.split("/")[-1]
    tmp_path = f"/tmp/{file_name}"
    cleaned_name = f"cleaned_{file_name.replace('.xlsx', '.csv')}"
    staging_path = f"/tmp/{cleaned_name}"

    try:
        # 5. Download từ RAW
        bucket_raw = storage_client.bucket(bucket_name)
        blob_raw = bucket_raw.blob(object_name)
        blob_raw.download_to_filename(tmp_path)

        # 6. Đọc & sanitize headers
        if lower.endswith(".csv"):
            df = pd.read_csv(tmp_path, encoding="utf-8-sig", dtype=str)
        else:
            df = pd.read_excel(tmp_path, dtype=str)

        df.columns = sanitize_and_dedupe_columns(df.columns)

        # 7. Upload file đã clean lên STAGING
        # gs://amaz-staging/manual_file/patients/cleaned_file.csv
        staging_object = f"{MANUAL_FILE_PREFIX}{file_key}/{cleaned_name}"
        df.to_csv(staging_path, index=False, encoding="utf-8")
        bucket_staging = storage_client.bucket(STAGING_BUCKET)
        blob_staging = bucket_staging.blob(staging_object)
        blob_staging.upload_from_filename(staging_path)
        staging_uri = f"gs://{STAGING_BUCKET}/{staging_object}"
        print(f"File moved to Staging: {staging_uri}")

        # 8. Load STAGING -> BQ staging table (WRITE_TRUNCATE, dùng subfolder làm table name)
        table_id = f"{PROJECT_ID}.{DATASET}.{file_key}"
        schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
            autodetect=False,
            allow_quoted_newlines=True,
            skip_leading_rows=1,
        )

        load_job = bq_client.load_table_from_uri(staging_uri, table_id, job_config=job_config)
        load_job.result()
        print(f"Staging load done: {table_id}")

        # 9. Chạy pipeline steps từ BQ config
        run_pipeline_steps(file_key)

        return (f"Process completed: {table_id}", 200)

    except Exception as e:
        print(f"Error processing file: {e}")
        return (str(e), 500)

    finally:
        for path in [tmp_path, staging_path]:
            if os.path.exists(path):
                os.remove(path)