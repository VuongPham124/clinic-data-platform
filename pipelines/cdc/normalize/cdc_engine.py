from table_processor import process_table

def run_all_tables(spark, contract, args):
    tables = (
        contract["tables"].keys()
        if args.tables == "ALL"
        else [t.strip() for t in args.tables.split(",") if t.strip()]
    )

    for table in tables:
        if table not in contract["tables"]:
            print(f"[WARN] table not in contract: {table}")
            continue

        try:
            process_table(
                spark,
                table,
                contract["tables"][table],
                args
            )
        except Exception as e:
            print(f"[ERROR] table {table}: {e}")
