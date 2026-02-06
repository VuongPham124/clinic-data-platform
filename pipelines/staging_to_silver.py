import json
import logging
import re
from datetime import datetime, timezone
from typing import Tuple
from typing import Dict, List
import argparse
from google.cloud import bigquery
from google.cloud import storage

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("staging_to_silver_bq")

# =====================================================
# CONFIG
# =====================================================
STAGING_BUCKET = "amaz-staging"
STAGING_PREFIX = "snapshot"

BQ_PROJECT = "wata-clinicdataplatform-gcp"
BQ_DATASET = "silver"

# =====================================================
# HELPERS
# =====================================================
def read_json(path: str) -> Dict:
    if path.startswith("gs://"):

        client = storage.Client()
        bucket_name, blob_path = path[5:].split("/", 1)
        blob = client.bucket(bucket_name).blob(blob_path)

        return json.loads(blob.download_as_text())
    else:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

def gcs_prefix_exists(
    storage_client: storage.Client,
    bucket: str,
    prefix: str
) -> bool:
    blobs = storage_client.list_blobs(
        bucket,
        prefix=prefix,
        max_results=1
    )
    return any(True for _ in blobs)
# =====================================================
# PARSE ARGS
# =====================================================
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--load-date", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--contract-path", required=True)
    return p.parse_args()
# =====================================================
# MAIN
# =====================================================
def run(
    load_date: str,
    run_id: str,
    contract_path: str,
):
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=BQ_PROJECT)

    contract = read_json(contract_path)
    tables_cfg = contract["tables"]

    # -------- Detect run --------
    

    logger.info(
        f"Detected STAGING run load_date={load_date}, run_id={run_id}"
    )

    started_at = datetime.now(timezone.utc).isoformat()

    tables_loaded = 0

    # -------- Process tables --------
    for full_name in tables_cfg.keys():
        schema, table = full_name.split(".")
        bq_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{schema}_{table}"

        gcs_prefix = (
            f"{STAGING_PREFIX}/{full_name}/"
            f"load_date={load_date}/run_id={run_id}/"
        )

        if not gcs_prefix_exists(
            storage_client,
            STAGING_BUCKET,
            gcs_prefix
        ):
            logger.warning(
                f"SKIP table={full_name} | no staging data"
            )
            continue

        source_uri = (
            f"gs://{STAGING_BUCKET}/{gcs_prefix}*.parquet"
        )

        logger.info(
            f"Loading table={bq_table_id} from {source_uri}"
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        load_job = bq_client.load_table_from_uri(
            source_uris=source_uri,
            destination=bq_table_id,
            job_config=job_config
        )

        load_job.result()

        table = bq_client.get_table(bq_table_id)

        logger.info(
            f"Finished table={bq_table_id} rows={table.num_rows}"
        )

        tables_loaded += 1

    finished_at = datetime.now(timezone.utc).isoformat()

    # -------- HISTORY --------
    history_record = {
        "pipeline": "staging_to_silver",
        "source": "snapshot",
        "load_date": load_date,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "tables_loaded": tables_loaded
    }

    history_path = (
        f"{STAGING_PREFIX}/_HISTORY/"
        f"load_date={load_date}/run_id={run_id}/"
        f"staging_to_silver.json"
    )

    storage_client.bucket(STAGING_BUCKET).blob(
        history_path
    ).upload_from_string(
        json.dumps(history_record, indent=2),
        content_type="application/json"
    )

    logger.info(
        f"Staging → Silver finished | tables_loaded={tables_loaded}"
    )
