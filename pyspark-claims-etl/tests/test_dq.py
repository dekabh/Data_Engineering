#!/.venv/bin/python
from __future__ import annotations
from pyspark.sql import functions as F

from ..src.claims_etl.claims_etl_transforms import normalize_policyholders, clean_claims, join_claims_policyholders
from ..src.claims_etl.claims_etl_dq import run_dq_checks

def test_dq_detects_ri_and_region_mismatch(spark):
    claims_raw = spark.createDataFrame(
        [
            ("CL_472", "PH_101", "East", "High", "1500.75", "2023-01-15"),
            ("RX_819", "PH_101", "West", "Low",  "4200.00", "2023-02-10"),  # mismatch
            ("CL_627", "PH_115", "North", "Low", "2800.25", "2023-04-20"), # unknown policyholder
        ],
        ["claim_id", "policyholder_id", "region", "claim_urgency", "claim_amount", "claim_date"],
    )
    policyholders_raw = spark.createDataFrame(
        [
            ("PH_101", "John Doe", "East"),
        ],
        ["policyholder_id", "policyholder_name", "region"],
    )

    ph_clean = normalize_policyholders(policyholders_raw)
    claims_clean = clean_claims(claims_raw)
    joined = join_claims_policyholders(claims_clean, ph_clean)

    report = run_dq_checks(claims_clean, ph_clean, joined)
    res = {r["rule_id"]: r for r in report["results"]}

    assert res["RI_CLAIMS_POLICYHOLDER"]["failed_count"] == 1
    assert res["CONSISTENCY_REGION_MATCH"]["failed_count"] == 1
    assert res["UNIQUENESS_CLAIM_ID"]["passed"] is True