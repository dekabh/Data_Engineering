#!/.venv/bin/pythonf
from __future__ import annotations
from datetime import date
from pyspark.sql import functions as F
from chispa import assert_df_equality

from ..src.claims_etl.claims_etl_transforms import (
    normalize_policyholders,
    clean_claims,
    join_claims_policyholders,
    build_claims_enriched_output,
)

def test_normalize_policyholders(spark):
    df = spark.createDataFrame(
        [
            (" PH_101 ", " John Doe ", " east "),
            ("PH_101", "John X", "EAST"),  # duplicate id -> keep first
            ("PH_237", "Jane Smith", "West"),
            (None, "Nobody", "North"),     # filtered
        ],
        ["policyholder_id", "policyholder_name", "region"],
    )
    out = normalize_policyholders(df)
    ids = set(r["policyholder_id"] for r in out.collect())
    assert ids == {"PH_101", "PH_237"}
    rows = {r["policyholder_id"]: r for r in out.collect()}
    assert rows["PH_101"]["region"] == "East"
    assert rows["PH_101"]["policyholder_name"] == "John Doe"

def test_clean_claims_and_join_and_aggregate(spark):
    claims_raw = spark.createDataFrame(
        [
            ("CL_472", "PH_101", "East", "High", "1500.75", "2023-01-15"),
            ("RX_819", "PH_101", "West", "Low",  "4200.00", "2023-02-10"),
            ("CX_235", "PH_237", "West", "High", "4500.50", "2023-03-05"),
            ("CL_627", "PH_115", "North", "Low", "2800.25", "2023-04-20"),
            ("RX_394", "PH_237", "South", "High","1800.00", "2023-05-12"),
            ("CL_153", "PH_152", "South", "Low", "5100.00", "2023-06-08"),
        ],
        ["claim_id", "policyholder_id", "region", "claim_urgency", "claim_amount", "claim_date"],
    )

    policyholders_raw = spark.createDataFrame(
        [
            ("PH_101", "John Doe", "East"),
            ("PH_237", "Jane Smith", "West"),
            ("PH_287", "Alice Brown", "North"),
            ("PH_152", "Bob Johnson", "South"),
        ],
        ["policyholder_id", "policyholder_name", "region"],
    )

    ph_clean = normalize_policyholders(policyholders_raw)
    claims_clean = clean_claims(claims_raw)

    assert "claim_priority" in claims_clean.columns
    assert claims_clean.select(F.min("claim_date")).collect()[0][0] == date(2023, 1, 15)

    joined = join_claims_policyholders(claims_clean, ph_clean)
    mismatch_count = joined.filter(
        (F.col("policyholder_id") == "PH_101") & (F.col("region") != F.col("policyholder_region"))
    ).count()
    assert mismatch_count == 1  # West vs East

    gold = policyholder_monthly_aggregates(joined).orderBy("policyholder_id", "year_month")
    row_jan = gold.filter((F.col("policyholder_id") == "PH_101") & (F.col("year_month") == "2023-01")).collect()[0]
    assert str(row_jan["total_amount"]) == "1500.75"
    row_feb = gold.filter((F.col("policyholder_id") == "PH_101") & (F.col("year_month") == "2023-02")).collect()[0]
    assert str(row_feb["total_amount"]) == "4200.00"