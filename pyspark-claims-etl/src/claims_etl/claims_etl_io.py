#!/.venv/bin/python
from __future__ import annotations
import logging
import os
from pathlib import Path
import shutil
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

def read_csv_with_schema(spark: SparkSession, path: str | Path, schema: StructType) -> DataFrame:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(str(path))
    )
    df.show(7, truncate=False)
    return df

def write_csv(
    gold_df: DataFrame,
    gold_path: str | Path,
    mode: str = "overwrite",
    header: bool = True,
    sep: str = ",",
) -> None:
    """
    Write a DataFrame to CSV.
    
    Behavior:
      - If 'year_month' or 'claim_period' is present in the DataFrame, we partition by it.
      - Otherwise, the output is written unpartitioned.
      - Uses sensible CSV defaults (UTF-8, quoted fields, newline separator, ISO date/timestamp).

    Args:
        gold_df: The DataFrame to write.
        gold_path: Target directory path for the CSV dataset.
        mode: Save mode: "overwrite", "append", "ignore", "error", or "errorifexists".
        header: Whether to include a header row in each CSV file.
        sep: Field delimiter (default: comma).

    Notes:
        - CSV is a text format; schema and types are not preserved in the files.
        - For dynamic partition overwrite (replace only touched partitions),
          ensure Spark is configured with:
              spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        - To produce exactly one file per partition, you would need to filter
          per partition and coalesce(1) per partition (not recommended for large data).
    """
    if gold_df is None:
        raise ValueError("gold_df must not be None")

    # Normalize path
    path_str = str(gold_path)

    # Identify a partition column if present
    partition_col: Optional[str] = None
    for candidate in ("year_month", "claim_period"):
        if candidate in gold_df.columns:
            partition_col = candidate
            break

    # Repartition by partition column to reduce small files per partition
    out_df = gold_df
    if partition_col is not None:
        out_df = gold_df.repartition(gold_df[partition_col])
    
    writer = (
        out_df.write
        .mode(mode)
        .format("csv")
        .option("header", str(header).lower())
        .option("sep", sep)
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "")
        .option("emptyValue", "")
        .option("lineSep", "\n")
        .option("charset", "UTF-8")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")
    )
    
    if partition_col is not None:
        writer = writer.partitionBy(partition_col)

    writer.save(path_str)
    logging.info("Wrote CSV to %s", path_str)
    
def merge_all_csv_parts_in_gold(spark: SparkSession, base_path: str = "data/output/gold", output_filename: str = "processed_claims.csv") -> None:
    """
    Recursively merges all part CSV files from subdirectories under `base_path` into a single CSV file.

    Args:
        base_path (str): Root directory containing folders with part CSV files.
        output_filename (str): Name of the final merged CSV file.

    Behavior:
        - Recursively finds all `part-*.csv` files under `base_path`.
        - Reads them into a single DataFrame.
        - Coalesces into one partition.
        - Writes a single CSV file to `base_path`.
    """

    # Recursively find all part CSV files
    part_files = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.startswith("part-") and file.endswith(".csv"):
                part_files.append(os.path.join(root, file))

    if not part_files:
        raise FileNotFoundError("No part CSV files found under the specified path.")

    # Read all part files into a single DataFrame
    df = spark.read.option("header", True).csv(part_files)

    # Coalesce to a single partition
    single_df = df.coalesce(1)

    # Temporary output path
    temp_output_path = os.path.join(base_path, "_merged_temp")

    # Write to temporary folder
    single_df.write.option("header", True).mode("overwrite").csv(temp_output_path)

    # Move the single part file to final output
    for file in os.listdir(temp_output_path):
        if file.startswith("part-") and file.endswith(".csv"):
            shutil.move(
                os.path.join(temp_output_path, file),
                os.path.join(base_path, output_filename)
            )
            break

    # Clean up temporary folder
    shutil.rmtree(temp_output_path)