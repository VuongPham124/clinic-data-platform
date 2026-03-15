# AMAZ File ETL — Deployment & Usage Guide

## Step 1 — Deploy Cloud Run Service

Run the following command from the directory containing the source code (`main.py`, `Dockerfile`, etc.):

```powershell
gcloud run deploy amaz-file-etl `
  --source . `
  --region us-central1 `
  --allow-unauthenticated `
  --project wata-clinicdataplatform-gcp `
  --set-env-vars PROJECT_ID=wata-clinicdataplatform-gcp","BQ_DATASET=manual","RAW_BUCKET=amaz-raw","STAGING_BUCKET=amaz-staging","BQ_SILVER_DATASET=silver","MANUAL_FILE_PREFIX=manual_file/
```

---

## Step 2 — Create the Pipeline Config Table in BigQuery

Open [BigQuery Console](https://console.cloud.google.com/bigquery), open the Editor, paste the full contents of `setup_pipeline_config.sql`, and click **Run**.

> This command creates the `manual.__pipeline_config` table and inserts all config queries for the 3 file templates.

---

## Step 3 — Upload Files to GCS

Excel files must be uploaded to the correct subfolder within `manual_file/`, with a `yyyy/MM/dd/HH/mm` timestamp for time-based organization:

```
amaz-raw/
  manual_file/
    medicine_categories/2026/03/06/21/26/    ← medicine categories file
    inventory/2026/03/06/21/26/              ← inventory import file
    patients/2026/03/06/21/26/               ← patient file
```

### Upload Command (PowerShell)

```powershell
$ts = Get-Date -Format "yyyy/MM/dd/HH/mm"
gcloud storage cp "Import Medicine Categories Template.xlsx" gs://amaz-raw/manual_file/medicine_categories/$ts/
gcloud storage cp "Import Medicines To Inventory Template.xlsx" gs://amaz-raw/manual_file/inventory/$ts/
gcloud storage cp "1.xlsx" gs://amaz-raw/manual_file/patients/$ts/
```

> **Note:** The file name does not matter — the pipeline routes by **subfolder** name. Files placed outside `manual_file/` or not in `.xlsx`/`.csv` format are silently ignored (HTTP 204).

---

## Step 4 — Verify Results

View Cloud Run logs:

```powershell
gcloud run services logs read amaz-file-etl --region us-central1 --limit 50
```

A successful log looks like:

```
file_key=patients, object=manual_file/patients/2026/03/06/21/26/1.xlsx
Staging load done: wata-clinicdataplatform-gcp.manual.patients
Running 2 pipeline step(s) for: patients
  Step 1 -> public_users ... Rows affected: 2
  Step 2 -> public_patients ... Rows affected: 2
```

---

## 3 Pre-configured File Templates

| Subfolder (`file_key`) | Steps | Destination Table |
|---|---|---|
| `medicine_categories` | 1 | `silver.public_medicines` (MERGE by `code`) |
| `inventory` | 2 | `silver.public_medicine_imports` → `silver.public_medicine_import_details` |
| `patients` | 2 | `silver.public_users` → `silver.public_patients` |

---

## Adding a New File Template

No redeployment required. Simply:

1. Create a new subfolder in `gs://amaz-raw/manual_file/<new_name>/`
2. INSERT a config row into `__pipeline_config` with `file_key` matching the subfolder name

```sql
INSERT INTO `wata-clinicdataplatform-gcp.manual.__pipeline_config`
  (file_key, step_order, target_table, query_template, is_active)
VALUES (
  'new_subfolder_name',
  1,
  'destination_table_name',
  """<MERGE or INSERT query here>""",
  TRUE
);
```

> `file_key` = subfolder name in lowercase; replace special characters with `_`.

---

## `__pipeline_config` Table Structure

| Column | Type | Description |
|---|---|---|
| `file_key` | STRING | Subfolder name (e.g., `patients`, `inventory`, `medicine_categories`) |
| `step_order` | INT64 | Execution order (1, 2, ...) |
| `target_table` | STRING | Destination table name in the silver dataset |
| `query_template` | STRING | Query with placeholders `{project}`, `{dataset}`, `{silver_dataset}` |
| `is_active` | BOOL | Enable/disable a step without deleting it |

### Query Template Placeholders

| Placeholder | Resolved Value |
|---|---|
| `{project}` | `wata-clinicdataplatform-gcp` |
| `{dataset}` | `manual` (staging dataset) |
| `{silver_dataset}` | `silver` (destination dataset) |

---
