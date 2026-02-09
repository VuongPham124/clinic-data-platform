from pyspark.sql.functions import lit
from cdc_normalizer import normalize_datastream_cdc
from schema_applier import apply_schema_casting, apply_timestamp_casting, build_invalid_condition
from io_utils import resolve_input_prefixes, write_manifest, gcs_path_exists
from datetime import datetime

def process_table(spark, table, table_cfg, args):
    prefixes = None
    try:
        prefixes = resolve_input_prefixes(
            args.raw_bucket, table,
            args.source_date_local, args.month, args.latest
        )

        # skip if no input
        if not any(gcs_path_exists(spark, p) for p in prefixes):
            print(f"[SKIP] table {table}: no input under {prefixes}")
            return

        # df_raw = spark.read.format("avro").load(prefixes)
        df_raw = (
            spark.read.format("avro")
            .option("datetimeRebaseMode", "CORRECTED")   # hoặc "LEGACY"
            .load(prefixes)
        )

        from pyspark.sql.functions import col

        df = normalize_datastream_cdc(df_raw, table, args.run_id, args.source_date_local)

        print("[DEBUG] after normalize null __lsn_num:", df.filter(col("__lsn_num").isNull()).count())

        df = apply_schema_casting(df, table_cfg)

        print("[DEBUG] after schema_casting null __lsn_num:", df.filter(col("__lsn_num").isNull()).count())

        df = apply_timestamp_casting(df, table_cfg)

        invalid_cond = build_invalid_condition(df, table_cfg)
        ok_df = df.filter(~invalid_cond)
        bad_df = df.filter(invalid_cond).withColumn("reason_code", lit("CONTRACT_VALIDATION_FAIL"))

        ok_cnt = ok_df.count()
        bad_cnt = bad_df.count()

        staging_path = (
            f"{args.staging_bucket}/cdc/{table.replace('.', '_')}/"
            f"source_date_local={args.source_date_local}/run_id={args.run_id}"
        )
        ok_df.coalesce(args.ok_files).write.mode("overwrite").parquet(staging_path)

        if bad_cnt > 0:
            quarantine_path = (
                f"{args.quarantine_bucket}/raw-to-staging-failed/cdc/"
                f"{table.replace('.', '_')}/"
                f"source_date_local={args.source_date_local}/run_id={args.run_id}"
            )
            bad_df.coalesce(args.bad_files)\
                .write.mode("overwrite")\
                .partitionBy("reason_code")\
                .parquet(quarantine_path)

        manifest = {
            "table": table,
            "run_id": args.run_id,
            "input": prefixes,
            "records": {"ok": ok_cnt, "quarantine": bad_cnt},
            "created_at": datetime.utcnow().isoformat()
        }

        manifest_path = (
            f"{args.staging_bucket}/_manifests/cdc/"
            f"{table.replace('.', '_')}/"
            f"source_date_local={args.source_date_local}/"
            f"run_id={args.run_id}.json"
        )
        write_manifest(manifest_path, manifest)

    except Exception as e:
        pinfo = prefixes if prefixes is not None else "<prefixes_not_resolved>"
        print(f"[ERROR] table {table}: {e} | input={pinfo}")

