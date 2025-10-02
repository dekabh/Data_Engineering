from __future__ import annotations
from ast import Tuple
from collections.abc import Iterable
import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
import requests
from claims_etl_constants import ALLOWED_REGIONS
from claims_etl_schemas import CLAIMS_SILVER_SCHEMA


def _trim(col: F.Column) -> F.Column:
    return F.trim(col)


def _title_case(col: F.Column) -> F.Column:
    return F.initcap(F.lower(col))


def normalize_policyholders(df: DataFrame) -> DataFrame:
    """
    Trim strings, normalize region (Title case), drop rows without policyholder_id, deduplicate by id.
    """
    out = (
        df.withColumn("policyholder_id", _trim(F.col("policyholder_id")))
        .withColumn("policyholder_name", _trim(F.col("policyholder_name")))
        .withColumn("region", _title_case(_trim(F.col("region"))))
        .filter(F.col("policyholder_id").isNotNull() & (F.length("policyholder_id") > 0))
    )
    w = Window.partitionBy("policyholder_id").orderBy(F.lit(1))
    out = out.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    return out


def normalize_claims_raw(df: DataFrame) -> DataFrame:
    """
    Normalize raw claims columns:
      - rename `claim_urgency` → `claim_priority`
      - keep `claim_type` if exists (else null)
      - parse date, cast amount to decimal(18,2)
      - normalize region to Title case
      - trim identifiers
    """
    out = df
    if "claim_urgency" in out.columns:
        out = out.withColumnRenamed("claim_urgency", "claim_priority")
    else:
        out = out.withColumn("claim_priority", F.lit(None).cast(T.StringType()))

    if "claim_type" not in out.columns:
        out = out.withColumn("claim_type", F.lit(None).cast(T.StringType()))

    out = (
        out.withColumn("claim_id", _trim(F.col("claim_id")))
        .withColumn("policyholder_id", _trim(F.col("policyholder_id")))
        .withColumn("region", _title_case(_trim(F.col("region"))))
        .withColumn("claim_type", _trim(F.col("claim_type")))
        .withColumn("claim_priority", _trim(F.col("claim_priority")))        
        .withColumn("claim_amount", F.regexp_replace(F.col("claim_amount"), ",", ""))
        .withColumn("claim_amount", F.when(F.col("claim_amount") == "", None).otherwise(F.col("claim_amount")))
        .withColumn("claim_amount", F.col("claim_amount").cast("decimal(18,2)"))
        .withColumn("claim_date", F.to_date(F.col("claim_date"), "yyyy-MM-dd"))
    )
    return out


def deduplicate_claims(df: DataFrame) -> DataFrame:
    """
    Deduplicate claims by `claim_id`. Keep latest by (claim_date DESC).
    """
    w = Window.partitionBy("claim_id").orderBy(F.col("claim_date").desc_nulls_last())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


def enforce_silver_schema(df: DataFrame) -> DataFrame:
    """
    Ensure df conforms to CLAIMS_SILVER_SCHEMA column names & types (add missing as null, order columns).
    """
    expected_cols = [f.name for f in CLAIMS_SILVER_SCHEMA.fields]
    out = df
    for f in CLAIMS_SILVER_SCHEMA.fields:
        if f.name not in out.columns:
            out = out.withColumn(f.name, F.lit(None).cast(f.dataType))
    return out.select(*expected_cols)


def clean_claims(df: DataFrame) -> DataFrame:
    """
    Full cleaning pipeline:
      - normalize raw
      - deduplicate
      - enforce schema
      - filter critical nulls
    """
    normalized = normalize_claims_raw(df)
    deduped = deduplicate_claims(normalized)
    enforced = enforce_silver_schema(deduped)

    cleaned = enforced.filter(
        F.col("claim_id").isNotNull()
        & F.col("policyholder_id").isNotNull()
        & F.col("claim_date").isNotNull()
        & F.col("claim_amount").isNotNull()
    ).withColumn("region", _title_case(F.col("region")))

    return cleaned


def join_claims_policyholders(claims: DataFrame, policyholders: DataFrame) -> DataFrame:
    """
    Left join claims with policyholders, adding region consistency boolean.
    """
    
    ph = policyholders.hint("broadcast")
    joined = (
        claims.join(
            ph.select(
                "policyholder_id",
                F.col("policyholder_name"),
                F.col("region").alias("policyholder_region")                
            ),
            on="policyholder_id",
            how="left",
        )
        .withColumn(
            "region_matches_policyholder",
            F.when(F.col("policyholder_region").isNull(), F.lit(None)).otherwise(
                F.col("region") == F.col("policyholder_region")
            ),
        )
    )
    joined.show(10, truncate=False)
    return joined


def build_claims_enriched_output(joined_df:DataFrame) -> DataFrame:
    """
    Accept a joined DataFrame (claims already joined to policyholders) and return the
    enriched output with the exact schema and rules:

    Output columns & rules:
      - claim_id (String): from input as-is
      - policyholder_name (String): from joined input
      - region (String): title-case region of the claim
      - claim_type (String): derived from claim_id prefix -> CL=Coinsurance, RX=Reinsurance, else null
      - claim_priority (String): 'Urgent' if claim_amount > 4000.0 else 'Normal'
      - claim_amount (Float): amount from input (cast)
      - claim_period (String): yyyy-MM from claim_date
      - source_system_id (String): first numeric part of claim_id (e.g., '123' from 'CL_123')
      - hash_id (String): MD4 hash of claim_id via https://api.hashify.net/hash/md4/hex?value=<claim_id>
                          using the 'Digest' field in the JSON response.
                          (Network failures result in null hash_id.)

    Assumptions about 'joined_df' input columns:
      - claim_id: String
      - policyholder_name: String
      - region: String
      - claim_amount: numeric or string
      - claim_date: Date or String(yyyy-MM-dd)

    Returns:
        DataFrame with the columns in the exact required order and types.
    """
    if joined_df is None:
        raise ValueError("joined DataFrame must not be None")

    #Derive claim_type from claim_id prefix
    joined_df = joined_df.withColumn(
        "claim_type", 
        F.when(F.col("claim_id").startswith("CL"), "Coinsurance")
        .when(F.col("claim_id").startswith("RX"), "Reinsurance")
        .otherwise("Unknown")
    )
    
    
    #Mark claim_priority based on claim_amount
    joined_df = joined_df.withColumn(
        "claim_priority",
        F.when(F.col("claim_amount").cast("float") > 4000, "Urgent").otherwise("Normal")
    )
    
    
    # Extract claim_period (YYYY-MM) from claim_date
    joined_df = joined_df.withColumn("claim_period", F.date_format("claim_date", "yyyy-MM"))
    
    
    # Extract numeric portion from claim_id for source_system_id
    joined_df = joined_df.withColumn("source_system_id", F.substring_index("claim_id", "_", -1))
    
    
    # Define UDF to fetch MD4 hash from external API
    def fetch_md4_hash(claim_id):
        try:
            response = requests.get(f"https://api.hashify.net/hash/md4/hex?value={claim_id}")
            if response.status_code == 200:
                return response.json().get("Digest", "")
        except Exception:
            return ""
        return ""

    fetch_md4_hash_udf = F.udf(fetch_md4_hash, T.StringType())
    
    
    # Apply UDF to get hash_id
    joined_df = joined_df.withColumn("hash_id", fetch_md4_hash_udf(F.col("claim_id")))

    # Select and rename columns as per output schema
    final_df = joined_df.select(
        "claim_id",
        "policyholder_name",
        F.col("region"),  # from claims_df
        "claim_type",
        "claim_priority",
        F.concat(F.lit("$"), F.format_number("claim_amount",2)).alias("claim_amount"),
        "claim_period",
        "source_system_id",
        "hash_id"
    )

    # Save the final DataFrame 
    final_df.coalesce(1)    
    final_df.show(10, truncate=False)

    return final_df






    