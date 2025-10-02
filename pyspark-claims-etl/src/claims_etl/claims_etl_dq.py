#!/.venv/bin/python
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from claims_etl_constants import ALLOWED_REGIONS, SEVERITY_ERROR, SEVERITY_WARNING

@dataclass
class DQRuleResult:
    rule_id: str
    description: str
    severity: str
    passed: bool
    failed_count: int
    sample_ids: list[str]


def _collect_sample_ids(df: DataFrame, id_col: str, limit: int = 10) -> list[str]:
    rows = df.select(id_col).where(F.col(id_col).isNotNull()).limit(limit).collect()
    return [r[0] for r in rows]


def dq_uniqueness_claim_id(df: DataFrame) -> DQRuleResult:
    dup = df.groupBy("claim_id").agg(F.count("*").alias("cnt")).filter(F.col("cnt") > 1)
    failed = dup

    # Robust check for empty DataFrame
    passed = failed.take(1) == []

    return DQRuleResult(
        rule_id="UNIQUENESS_CLAIM_ID",
        description="claim_id must be unique",
        severity=SEVERITY_ERROR,
        passed=passed,
        failed_count=failed.count(),
        sample_ids=_collect_sample_ids(failed, "claim_id"),
    )


def dq_not_nulls(df: DataFrame) -> List[DQRuleResult]:
    rules = []
    for col in ["claim_id", "policyholder_id", "claim_date", "claim_amount"]:
        bad = df.filter(F.col(col).isNull())

        #Avoid recomputation
        bad_cached = bad.cache()
        passed = bad_cached.take(1) == []

        rules.append(
            DQRuleResult(
                rule_id=f"NOT_NULL_{col.upper()}",
                description=f"{col} must not be null",
                severity=SEVERITY_ERROR,
                passed=passed,
                failed_count=bad_cached.count(),
                sample_ids=_collect_sample_ids(bad_cached, "claim_id"),
            )
        )
    return rules


def dq_region_domain(df: DataFrame) -> DQRuleResult:    
    # Normalize region column to uppercase and filter invalid values
    bad = df.filter(~F.upper(F.col("region")).isin(list(ALLOWED_REGIONS)))
    bad_cached = bad.cache()

    # Check if DataFrame is empty
    passed = bad_cached.take(1) == []

    return DQRuleResult(
        rule_id="DOMAIN_REGION",
        description=f"region must be in {sorted(ALLOWED_REGIONS)}",
        severity=SEVERITY_ERROR,
        passed=passed,
        failed_count=bad_cached.count(),
        sample_ids=_collect_sample_ids(bad_cached, "claim_id"),
    )


def dq_amount_non_negative(df: DataFrame) -> DQRuleResult:
    bad = df.filter(F.col("claim_amount") < 0)
    failed_count = bad.count()
    return DQRuleResult(
        rule_id="VALID_AMOUNT_NON_NEGATIVE",
        description="claim_amount must be >= 0",
        severity=SEVERITY_ERROR,
        passed=bad.isEmpty(),
        failed_count=failed_count,
        sample_ids=_collect_sample_ids(bad, "claim_id"),
    )


def dq_referential_integrity(claims: DataFrame, policyholders: DataFrame) -> DQRuleResult:
    ph_ids = policyholders.select("policyholder_id").dropDuplicates()
    missing = claims.join(ph_ids, on="policyholder_id", how="left_anti").select("policyholder_id").dropDuplicates()
    return DQRuleResult(
        rule_id="RI_CLAIMS_POLICYHOLDER",
        description="Every claim.policyholder_id must exist in policyholders",
        severity=SEVERITY_ERROR,
        passed=missing.isEmpty(),
        failed_count=missing.count(),
        sample_ids=_collect_sample_ids(missing, "policyholder_id"),
    )


def dq_region_consistency(joined: DataFrame) -> DQRuleResult:
    mismatch = joined.filter((F.col("policyholder_region").isNotNull()) & (F.col("region") != F.col("policyholder_region")))
    return DQRuleResult(
        rule_id="CONSISTENCY_REGION_MATCH",
        description="Claim region should match policyholder region (warning)",
        severity=SEVERITY_WARNING,
        passed=mismatch.isEmpty(),
        failed_count=mismatch.count(),
        sample_ids=_collect_sample_ids(mismatch, "claim_id"),
    )


def run_dq_checks(
    claims_clean: DataFrame,
    policyholders_clean: DataFrame,
    joined: DataFrame
) -> Dict[str, Any]:
    results: list[DQRuleResult] = []
    results.append(dq_uniqueness_claim_id(claims_clean))
    results.extend(dq_not_nulls(claims_clean))
    results.append(dq_region_domain(claims_clean))
    results.append(dq_amount_non_negative(claims_clean))
    results.append(dq_referential_integrity(claims_clean, policyholders_clean))
    results.append(dq_region_consistency(joined))

    return {
        "summary": {
            "total_rules": len(results),
            "errors": sum(1 for r in results if (not r.passed) and r.severity == SEVERITY_ERROR),
            "warnings": sum(1 for r in results if (not r.passed) and r.severity == SEVERITY_WARNING),
        },
        "results": [asdict(r) for r in results],
    }