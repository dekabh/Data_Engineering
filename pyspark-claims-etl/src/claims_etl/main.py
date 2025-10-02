#!/.venv/bin/python
from __future__ import annotations
from urllib.parse import quote


import os
import shutil
from pathlib import Path
import argparse
from pyspark.sql import functions as F
from claims_etl_io import merge_all_csv_parts_in_gold, read_csv_with_schema, write_csv
from claims_etl_schemas import CLAIMS_INPUT_SCHEMA, POLICYHOLDER_SCHEMA
from claims_etl_transforms import (
    normalize_policyholders,
    clean_claims,
    join_claims_policyholders,
    build_claims_enriched_output
)
from claims_etl_dq import run_dq_checks
from claims_etl_utils import (
    get_logger,
    get_run_id,
    get_spark,
    seed_sample_data,
    safe_mkdir,
    write_json,
    log_df_info,
)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySpark Claims ETL (with DQ & Logging)")
    parser.add_argument("--input-dir", type=str, required=True, help="Input directory (CSV files)")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory (Parquet)")
    parser.add_argument("--mode", type=str, choices=["overwrite", "append"], default="overwrite")
    parser.add_argument(
        "--fail-on-dq-error",
        type=lambda x: str(x).lower() in {"1", "true", "yes", "y"},
        default=True,
        help="Fail the job if any DQ errors occur (warnings never fail). Default: true",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default=None,
        help="Directory for rotating file logs (default: <output-dir>/../logs).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Prepare logging
    run_id = get_run_id()
    default_log_dir = Path(args.output_dir).parent / "logs"
    log_dir = args.log_dir or str(default_log_dir)
    log = get_logger("claims_etl", log_dir=log_dir, run_id=run_id)
    log.info("Job started. run_id=%s", run_id)

    # Seed example data if missing
    seed_sample_data(args.input_dir)

    spark = None
    try:
        spark = get_spark("claims-etl")
        log.info("Spark version: %s", spark.version)

        input_dir = Path(args.input_dir)
        output_dir = Path(args.output_dir)

        # ---------- Raw Layer ----------
        claims_raw = read_csv_with_schema(spark, input_dir / "claims_data.csv", CLAIMS_INPUT_SCHEMA)
        policyholders_raw = read_csv_with_schema(spark, input_dir / "policyholder_data.csv", POLICYHOLDER_SCHEMA)
        log_df_info(log, "claims_raw", claims_raw)
        log_df_info(log, "policyholders_raw", policyholders_raw)

        # ---------- Clean Layer ----------
        policyholders_clean = normalize_policyholders(policyholders_raw).cache()
        policyholders_clean.show(7, truncate=False)
        claims_clean = clean_claims(claims_raw).cache()
        claims_clean.show(5, truncate=False)
        log_df_info(log, "policyholders_clean", policyholders_clean)
        log_df_info(log, "claims_clean", claims_clean)

        # ---------- Join ----------
        joined = join_claims_policyholders(claims_clean, policyholders_clean).cache()
        log_df_info(log, "joined", joined)

        # ---------- Data Quality Check ----------
        report = run_dq_checks(claims_clean, policyholders_clean,joined)
        dq_path = output_dir / "dq_reports" / "report.json"
        write_json(dq_path, report)
        log.info("DQ Summary: %s", report["summary"])

        # Fail on errors if configured
        if args.fail_on_dq_error and report["summary"]["errors"] > 0:
            log.error("DQ errors detected. Failing job. See report at %s", dq_path)
            raise SystemExit(1)

        # ---------- Transformed Data -----------
        gold_df = build_claims_enriched_output(joined)
        gold_path = output_dir / "gold"
        if os.path.exists(gold_path):
            shutil.rmtree(gold_path,ignore_errors=False)
        safe_mkdir(gold_path)
        
        #gold_df.write.option("header", True).mode("overwrite").csv(f"{gold_path}/processed_claims")

        write_csv(gold_df, gold_path)
        
        # Merging small files into one per partition is not implemented here due to complexity and performance concerns.
        # If needed, this can be done with additional logic to filter and coalesce per partition.
        # For large datasets, it's recommended to handle small files at the storage level or use formats like Parquet/ORC.
        # Example (not recommended for large data):
        merge_all_csv_parts_in_gold(spark, base_path=str(gold_path), output_filename="processed_claims.csv")
        


        log.info("Processed claims data has been saved to %s", gold_path)

    finally:
        if spark:
            spark.stop()
            log.info("Spark session stopped.")


if __name__ == "__main__":
    main()