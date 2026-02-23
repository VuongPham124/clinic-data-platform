#!/usr/bin/env bash
set -euo pipefail

# =========================
# Usage:
#   ./run_load_merge.sh <SRC_DATE> <RUN_ID> [CONTRACT_PATH] [ONLY_TABLE]
#
# Args:
#   SRC_DATE      : source_date_local in YYYY-MM-DD (partition key)
#   RUN_ID        : batch identity (must match the GCS folder run_id=...)
#   CONTRACT_PATH : contract json file path (default: contract/cdc_staging_contract_v1.json)
#   ONLY_TABLE    : (optional) process only this one table_id (e.g. public_users)
#
# Examples:
#   # run all tables in contract:
#   ./run_load_merge.sh 2026-01-27 prod_20260127 contract/cdc_staging_contract_v1.json
#
#   # quick test one table only:
#   ./run_load_merge.sh 2026-01-27 prod_20260127 contract/cdc_staging_contract_v1.json public_users
# =========================

# --- ADDED: allow overriding via environment variables (useful for Composer/CI)
# Example:
#   export PROJECT_ID="my-project"
#   export LOCATION="us-central1"
#   export STAGING_BUCKET="my-staging-bucket"
# If not set, fall back to the defaults below.
: "${PROJECT_ID:=xxx}"
: "${LOCATION:=us-central1}"
: "${STAGING_BUCKET:=xxx}"

SRC_DATE="${1:?Missing SRC_DATE (YYYY-MM-DD)}"
RUN_ID="${2:?Missing RUN_ID}"
CONTRACT_PATH="${3:-contract/cdc_staging_contract_v1.json}"

# --- ADDED: quick test mode - process only one table if provided
# Use table_id in the same format as your folders: public.users -> public_users
ONLY_TABLE="${4:-}"
# ------------------------------------------------------------

echo "[INFO] PROJECT_ID=${PROJECT_ID}"
echo "[INFO] LOCATION=${LOCATION}"
echo "[INFO] STAGING_BUCKET=${STAGING_BUCKET}"
echo "[INFO] SRC_DATE=${SRC_DATE}"
echo "[INFO] RUN_ID=${RUN_ID}"
echo "[INFO] CONTRACT_PATH=${CONTRACT_PATH}"
echo "[INFO] ONLY_TABLE=${ONLY_TABLE:-<all>}"  # ADDED

# ---------- Input validation (avoid SQL injection / unsafe identifiers) ----------
if [[ ! "${SRC_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "[FATAL] SRC_DATE must be YYYY-MM-DD. Got: ${SRC_DATE}"
  exit 2
fi
if [[ ! "${RUN_ID}" =~ ^[a-z0-9_]+$ ]]; then
  echo "[FATAL] RUN_ID contains invalid characters. Allowed: [a-z0-9_]"
  exit 2
fi
if [ -n "${ONLY_TABLE}" ] && [[ ! "${ONLY_TABLE}" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "[FATAL] ONLY_TABLE contains invalid characters. Allowed: [A-Za-z0-9_]"
  exit 2
fi

# ---------- Validate contract ----------
if [ ! -f "${CONTRACT_PATH}" ]; then
  echo "[FATAL] Contract file not found: ${CONTRACT_PATH}"
  exit 1
fi
if [ ! -s "${CONTRACT_PATH}" ]; then
  echo "[FATAL] Contract file is empty: ${CONTRACT_PATH}"
  exit 1
fi

python3 - <<PY
import json
with open("${CONTRACT_PATH}", "r", encoding="utf-8") as f:
    json.load(f)
print("[INFO] Contract JSON valid")
PY

# ---------- Schemas + watermark ----------
bq --location="$LOCATION" query --use_legacy_sql=false "
CREATE SCHEMA IF NOT EXISTS \`${PROJECT_ID}.bq_stage\`;
CREATE SCHEMA IF NOT EXISTS \`${PROJECT_ID}.silver\`;
CREATE SCHEMA IF NOT EXISTS \`${PROJECT_ID}.silver_meta\`;

CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.silver_meta.watermarks\` (
  table_name STRING NOT NULL,
  last_lsn_num INT64,
  last_inferred_seq INT64,
  last_commit_ts TIMESTAMP,
  last_run_id STRING,
  updated_at TIMESTAMP
);

ALTER TABLE \`${PROJECT_ID}.silver_meta.watermarks\`
  ADD COLUMN IF NOT EXISTS last_inferred_seq INT64;
" >/dev/null

# ---------- Build table config from contract ----------
# CFG file (pipe-delimited): table_id|pk_csv|business_cols_csv|timestamp_fields_csv|decimal_fields_csv
# FIX: Do NOT use TAB here because bash `read` treats TAB as whitespace and collapses empty fields.
#      That caused decimal_fields to shift into TS_CSV when timestamp_fields is empty.
python3 - <<PY > /tmp/table_cfg.tsv
import json
with open("${CONTRACT_PATH}", "r", encoding="utf-8") as f:
    c = json.load(f)

tables = c.get("tables", {})
if not tables:
    raise SystemExit("contract.tables empty")

def pk(cfg):
    for k in ("primary_key","pk","primary_keys"):
        v = cfg.get(k)
        if v:
            return v if isinstance(v,list) else [v]
    return ["id"]

def cols(cfg):
    if isinstance(cfg.get("schema"), dict) and cfg["schema"]:
        return list(cfg["schema"].keys())
    if isinstance(cfg.get("columns"), list) and cfg["columns"]:
        out=[]
        for x in cfg["columns"]:
            out.append(x["name"] if isinstance(x,dict) and "name" in x else x)
        return out
    return []

import re
safe_ident = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def validate_ident_list(values, kind, table_name):
    bad = [v for v in values if not safe_ident.match(v)]
    if bad:
        raise SystemExit(f"Invalid {kind} in contract for {table_name}: {bad}")

for t,cfg in tables.items():
    # Validate table identifier and all column names to prevent SQL injection
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?", t):
        raise SystemExit(f"Invalid table name in contract: {t}")
    # ADDED: decimal_fields (Option A) for numeric casting in Silver merge.
    # Fallback: infer decimal/numeric columns from schema types in case decimal_fields is missing.
    dec_fields = list(cfg.get("decimal_fields") or [])
    schema = cfg.get("schema") if isinstance(cfg.get("schema"), dict) else {}
    inferred_dec = [
        k for k, v in schema.items()
        if isinstance(v, str) and re.search(r"(decimal|numeric)", v, re.IGNORECASE)
    ]
    if inferred_dec:
        dec_fields = sorted(set(dec_fields).union(inferred_dec))
    validate_ident_list(pk(cfg), "primary_key", t)
    validate_ident_list(cols(cfg), "columns", t)
    validate_ident_list((cfg.get("timestamp_fields") or []), "timestamp_fields", t)
    validate_ident_list(dec_fields, "decimal_fields", t)
    print(
        t.replace(".","_"),
        ",".join(pk(cfg)),
        ",".join(cols(cfg)),
        ",".join((cfg.get("timestamp_fields") or [])),
        ",".join(dec_fields),
        sep="|"
    )
PY

echo "[INFO] Tables in contract: $(wc -l < /tmp/table_cfg.tsv)"

# ---------- Helper: build SQL parts JSON safely (NO backtick expansion) ----------
build_sql_parts_json() {
  PK_CSV="$1" COLS_CSV="$2" python3 - <<'PY'
import os, json

pk=[x.strip() for x in os.environ["PK_CSV"].split(",") if x.strip()] or ["id"]
cols=[x.strip() for x in os.environ["COLS_CSV"].split(",") if x.strip()]
if not cols:
    raise SystemExit("NO_BUSINESS_COLS")

bt="`"
pk_set=set(pk)

part=", ".join(f"{bt}{x}{bt}" for x in pk)
join=" AND ".join(f"T.{bt}{x}{bt}=S.{bt}{x}{bt}" for x in pk)

upd=[f"{bt}{c}{bt}=S.{bt}{c}{bt}" for c in cols if c not in pk_set]
upd += ["is_deleted=S.is_deleted","__lsn_num=S.__lsn_num","__commit_ts=S.__commit_ts"]

icol=", ".join([f"{bt}{c}{bt}" for c in cols+["is_deleted","__lsn_num","__commit_ts"]])
ival=", ".join([f"S.{bt}{c}{bt}" for c in cols] + ["S.is_deleted","S.__lsn_num","S.__commit_ts"])

print(json.dumps({"part":part,"join":join,"upd":",\n      ".join(upd),"icol":icol,"ival":ival}, ensure_ascii=False))
PY
}

# ---------- Helper: business select list safely ----------
# If TS_FIELDS_CSV includes a column, we will parse/cast to TIMESTAMP robustly
# to handle stage strings like "{2022-03-11 13:56:18.284224, 0}" and also already-TIMESTAMP values.
build_business_select() {
  COLS_CSV="$1" TS_FIELDS_CSV="${2:-}" DEC_FIELDS_CSV="${3:-}" python3 - <<'PY'  # ADDED: DEC_FIELDS_CSV
import os

cols=[c.strip() for c in os.environ["COLS_CSV"].split(",") if c.strip()]
ts_fields=[c.strip() for c in os.environ.get("TS_FIELDS_CSV","").split(",") if c.strip()]
dec_fields=[c.strip() for c in os.environ.get("DEC_FIELDS_CSV","").split(",") if c.strip()]  # ADDED
ts_set=set(ts_fields)
dec_set=set(dec_fields)  # ADDED
bt="`"

out=[]
for c in cols:
    # ADDED: prioritize decimal casting over timestamp string casting
    if c in dec_set:
        # ADDED: robust NUMERIC casting for decimal fields (Option A)
        # Handles plain numeric strings and tuple-like strings such as '{123.45, 0}' by extracting the first numeric token.
        expr=(
            "SAFE_CAST(REGEXP_EXTRACT(CAST(s.{bt}{c}{bt} AS STRING), r'-?\\d+(?:\\.\\d+)?') AS NUMERIC) AS {bt}{c}{bt}"
        ).format(bt=bt, c=c)
        out.append(expr)
    # elif c in ts_set:
    #     # NOTE (TEMP FIX): keep timestamp_fields as STRING when building Silver rows.
    #     # This avoids BigQuery errors like: "TIMESTAMP cannot be assigned to <col>, which has type STRING"
    #     # when the existing silver table schema still has STRING columns.
    #     expr=f"CAST(s.{bt}{c}{bt} AS STRING) AS {bt}{c}{bt}"
    #     out.append(expr)
    elif c in ts_set:
      # Normalize timestamp string like:
      # "2024-03-31 07:34:52.766887" or "{2026-02-03 15:04:54.174359, 0}"
      expr = (
          "FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', "
          "SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', "
          "REGEXP_EXTRACT(CAST(s.{bt}{c}{bt} AS STRING), "
          "r'(\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}(?:\\.\\d{{1,6}})?)'), "
          "'Asia/Ho_Chi_Minh')) AS {bt}{c}{bt}"
      ).format(bt=bt, c=c)
      out.append(expr)
    else:
        out.append(f"s.{bt}{c}{bt}")
print(", ".join(out))
PY
}

# ---------- Loop ----------
# FIX: '|' keeps empty fields (tab is whitespace -> collapses)
while IFS='|' read -r TABLE_ID PK_CSV COLS_CSV TS_CSV DEC_CSV; do  # ADDED: DEC_CSV
  [ -z "$TABLE_ID" ] && continue

  # --- ADDED: fast test - run only one table if ONLY_TABLE is set
  if [ -n "${ONLY_TABLE}" ] && [ "${TABLE_ID}" != "${ONLY_TABLE}" ]; then
    continue
  fi
  # -----------------------------------------------------------

  GCS_URI="gs://${STAGING_BUCKET}/cdc/${TABLE_ID}/source_date_local=${SRC_DATE}/run_id=${RUN_ID}/*.parquet"
  if ! gsutil -q ls "$GCS_URI" >/dev/null 2>&1; then
    echo "[SKIP] ${TABLE_ID} no parquet: ${GCS_URI}"
    continue
  fi

  echo "=============================="
  echo "TABLE: ${TABLE_ID}"
  echo "PK:    ${PK_CSV}"
  echo "GCS:   ${GCS_URI}"
  echo "=============================="

  # CLI vs SQL table ids
  TEMP_CLI="${PROJECT_ID}:bq_stage.${TABLE_ID}_cdc__load_${RUN_ID//[^a-zA-Z0-9_]/_}"
  TEMP_SQL="${PROJECT_ID}.bq_stage.${TABLE_ID}_cdc__load_${RUN_ID//[^a-zA-Z0-9_]/_}"
  STAGE_SQL="${PROJECT_ID}.bq_stage.${TABLE_ID}_cdc"
  SILVER_SQL="${PROJECT_ID}.silver.${TABLE_ID}"

  # ----- Load Parquet into TEMP (replace) -----
  echo "=== LOAD ${TABLE_ID} -> TEMP ${TEMP_CLI} (REPLACE) ==="
  if ! bq --location="$LOCATION" load \
      --replace \
      --source_format=PARQUET \
      --autodetect \
      "${TEMP_CLI}" \
      "$GCS_URI" > /tmp/bq_load_out.txt 2>&1; then
    echo "[ERROR] bq load failed for ${TABLE_ID}"
    cat /tmp/bq_load_out.txt
    exit 1
  fi

  # ----- Ensure stage table exists -----
  echo "=== ENSURE bq_stage.${TABLE_ID}_cdc exists ==="
  if ! bq --location="$LOCATION" query --use_legacy_sql=false "
  CREATE TABLE IF NOT EXISTS \`${STAGE_SQL}\`
  AS SELECT * FROM \`${TEMP_SQL}\` WHERE FALSE;
  " > /tmp/bq_ensure_out.txt 2>&1; then
    echo "[ERROR] ensure stage table failed for ${TABLE_ID}"
    cat /tmp/bq_ensure_out.txt
    exit 1
  fi

  # ----- Idempotent stage upsert by (SRC_DATE, RUN_ID) -----
  echo "=== UPSERT bq_stage.${TABLE_ID}_cdc (idempotent by src_date+run_id) ==="
  if ! bq --location="$LOCATION" query --use_legacy_sql=false "
  DELETE FROM \`${STAGE_SQL}\`
  WHERE CAST(__source_date_local AS STRING)='${SRC_DATE}'
    AND CAST(__run_id AS STRING)='${RUN_ID}';

  DECLARE insert_expr STRING;
  SET insert_expr = (
    SELECT STRING_AGG(
      CASE
        WHEN tc.column_name IS NULL THEN
          FORMAT('CAST(NULL AS %s) AS \`%s\`', sc.data_type, sc.column_name)
        WHEN sc.data_type = 'STRING' THEN
          FORMAT('CAST(t.\`%s\` AS STRING) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'INT64' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS INT64) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'NUMERIC' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS NUMERIC) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'BIGNUMERIC' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS BIGNUMERIC) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'FLOAT64' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS FLOAT64) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'BOOL' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS BOOL) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'TIMESTAMP' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS TIMESTAMP) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'DATE' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS DATE) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'DATETIME' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS DATETIME) AS \`%s\`', sc.column_name, sc.column_name)
        WHEN sc.data_type = 'TIME' THEN
          FORMAT('SAFE_CAST(t.\`%s\` AS TIME) AS \`%s\`', sc.column_name, sc.column_name)
        ELSE
          FORMAT('t.\`%s\` AS \`%s\`', sc.column_name, sc.column_name)
      END
      ORDER BY sc.ordinal_position
    )
    FROM \`${PROJECT_ID}.bq_stage.INFORMATION_SCHEMA.COLUMNS\` sc
    LEFT JOIN \`${PROJECT_ID}.bq_stage.INFORMATION_SCHEMA.COLUMNS\` tc
      ON tc.table_name = '${TABLE_ID}_cdc__load_${RUN_ID//[^a-zA-Z0-9_]/_}'
     AND tc.column_name = sc.column_name
    WHERE sc.table_name = '${TABLE_ID}_cdc'
  );

  EXECUTE IMMEDIATE FORMAT(
    'INSERT INTO \`%s\` SELECT %s FROM \`%s\` t',
    '${STAGE_SQL}',
    insert_expr,
    '${TEMP_SQL}'
  );
  " > /tmp/bq_stage_upsert_out.txt 2>&1; then
    echo "[ERROR] stage upsert failed for ${TABLE_ID}"
    cat /tmp/bq_stage_upsert_out.txt
    exit 1
  fi

  # ----- Drop TEMP -----
  bq --location="$LOCATION" rm -f -t "${TEMP_CLI}" >/dev/null 2>&1 || true

  # ----- Build SQL pieces from contract -----
  SQL_JSON="$(build_sql_parts_json "$PK_CSV" "$COLS_CSV" 2>/tmp/sql_parts_err.txt || true)"
  if [ -z "$(echo "$SQL_JSON" | tr -d '[:space:]')" ]; then
    echo "[ERROR] Failed to build SQL parts for ${TABLE_ID}"
    cat /tmp/sql_parts_err.txt || true
    echo "PK=${PK_CSV}"
    echo "COLS=${COLS_CSV}"
    exit 1
  fi

  PART="$(printf "%s" "$SQL_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['part'])")"
  JOIN="$(printf "%s" "$SQL_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['join'])")"
  UPD="$(printf "%s" "$SQL_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['upd'])")"
  ICOL="$(printf "%s" "$SQL_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['icol'])")"
  IVAL="$(printf "%s" "$SQL_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['ival'])")"

  BSEL="$(build_business_select "$COLS_CSV" "$TS_CSV" "$DEC_CSV")"  # ADDED: pass decimal fields

  # ----- MERGE silver (CDC vs inferred/backfill separated) -----
  MERGE_SQL="
DECLARE wm_lsn INT64 DEFAULT (
  SELECT IFNULL(MAX(last_lsn_num),0)
  FROM \`${PROJECT_ID}.silver_meta.watermarks\`
  WHERE table_name='${TABLE_ID}'
);

DECLARE wm_inf INT64 DEFAULT (
  SELECT IFNULL(MAX(last_inferred_seq),0)
  FROM \`${PROJECT_ID}.silver_meta.watermarks\`
  WHERE table_name='${TABLE_ID}'
);

CREATE TABLE IF NOT EXISTS \`${SILVER_SQL}\` AS
SELECT
  ${BSEL},
  CAST(s.__is_deleted AS BOOL) AS is_deleted,
  SAFE_CAST(s.__lsn_num AS INT64) AS __lsn_num,
  CAST(s.__commit_ts AS TIMESTAMP) AS __commit_ts
FROM (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY ${PART}
      ORDER BY SAFE_CAST(s.__lsn_num AS INT64) DESC, s.__commit_ts DESC, s.__run_id DESC
    ) rn
  FROM \`${STAGE_SQL}\` s
) s
WHERE rn = 1;

MERGE \`${SILVER_SQL}\` T
USING (
  SELECT * EXCEPT(rn) FROM (
    -- CDC-real
    SELECT
      ${BSEL},
      CAST(s.__is_deleted AS BOOL) AS is_deleted,
      SAFE_CAST(s.__lsn_num AS INT64) AS __lsn_num,
      CAST(s.__commit_ts AS TIMESTAMP) AS __commit_ts,
      ROW_NUMBER() OVER (
        PARTITION BY ${PART}
        ORDER BY SAFE_CAST(s.__lsn_num AS INT64) DESC, s.__commit_ts DESC, s.__run_id DESC
      ) rn
    FROM \`${STAGE_SQL}\` s
    WHERE COALESCE(CAST(s.__lsn_num_inferred AS BOOL), FALSE) = FALSE
      AND SAFE_CAST(s.__lsn_num AS INT64) > wm_lsn

    UNION ALL

    -- inferred/backfill
    SELECT
      ${BSEL},
      CAST(s.__is_deleted AS BOOL) AS is_deleted,
      SAFE_CAST(s.__lsn_num AS INT64) AS __lsn_num,
      CAST(s.__commit_ts AS TIMESTAMP) AS __commit_ts,
      ROW_NUMBER() OVER (
        PARTITION BY ${PART}
        ORDER BY SAFE_CAST(s.__lsn_num AS INT64) DESC, s.__commit_ts DESC, s.__run_id DESC
      ) rn
    FROM \`${STAGE_SQL}\` s
    WHERE COALESCE(CAST(s.__lsn_num_inferred AS BOOL), TRUE) = TRUE
      AND SAFE_CAST(s.__lsn_num AS INT64) > wm_inf
  )
  WHERE rn = 1
) S
ON ${JOIN}

WHEN MATCHED AND S.__lsn_num > T.__lsn_num THEN
  UPDATE SET
      ${UPD}

WHEN NOT MATCHED THEN
  INSERT (${ICOL})
  VALUES (${IVAL})
;

MERGE \`${PROJECT_ID}.silver_meta.watermarks\` W
USING (
  SELECT
    '${TABLE_ID}' AS table_name,
    (SELECT MAX(SAFE_CAST(__lsn_num AS INT64))
     FROM \`${STAGE_SQL}\`
     WHERE COALESCE(CAST(__lsn_num_inferred AS BOOL), FALSE) = FALSE
    ) AS last_lsn_num,
    (SELECT MAX(SAFE_CAST(__lsn_num AS INT64))
     FROM \`${STAGE_SQL}\`
     WHERE COALESCE(CAST(__lsn_num_inferred AS BOOL), TRUE) = TRUE
    ) AS last_inferred_seq,
    (SELECT MAX(CAST(__commit_ts AS TIMESTAMP)) FROM \`${STAGE_SQL}\`) AS last_commit_ts,
    '${RUN_ID}' AS last_run_id,
    CURRENT_TIMESTAMP() AS updated_at
) X
ON W.table_name = X.table_name

WHEN MATCHED THEN UPDATE SET
  last_lsn_num = COALESCE(X.last_lsn_num, W.last_lsn_num),
  last_inferred_seq = COALESCE(X.last_inferred_seq, W.last_inferred_seq),
  last_commit_ts = COALESCE(X.last_commit_ts, W.last_commit_ts),
  last_run_id = X.last_run_id,
  updated_at = X.updated_at

WHEN NOT MATCHED THEN
  INSERT (table_name, last_lsn_num, last_inferred_seq, last_commit_ts, last_run_id, updated_at)
  VALUES (X.table_name, X.last_lsn_num, X.last_inferred_seq, X.last_commit_ts, X.last_run_id, X.updated_at);
"

  echo "=== MERGE ${TABLE_ID} -> silver.${TABLE_ID} ==="
  if ! bq --location="$LOCATION" query --use_legacy_sql=false "$MERGE_SQL" \
      > /tmp/bq_merge_out.txt 2>&1; then
    echo "[ERROR] MERGE failed for ${TABLE_ID}"
    cat /tmp/bq_merge_out.txt
    echo "---- SQL head (first 60 lines) ----"
    printf "%s\n" "$MERGE_SQL" | nl -ba | sed -n '1,60p'
    exit 1
  fi

  echo "[OK] ${TABLE_ID} done."

done < /tmp/table_cfg.tsv

echo "DONE."
