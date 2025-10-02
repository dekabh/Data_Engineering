from __future__ import annotations

"""
claims_etl package

PySpark-based ETL for processing insurance claims and policyholder data.

Modules included:
- claims_etl_schemas:         Strongly-typed Spark schemas for input and curated layers
- claims_etl_transforms:      Pure, testable transformations (cleaning, joining, aggregations)
- claims_etl_dq:              Data Quality (DQ) checks (uniqueness, domain, validity, RI, consistency)
- claims_etl_io:              Schema-aware CSV read and write helpers
- claims_etl_utils:           Logging, Spark session factory, run_id helpers, sample data seeding
- main:            CLI entry point for end-to-end execution

Typical usage:

    from claims_etl import (
        clean_claims,
        normalize_policyholders,
        join_claims_policyholders,
        build_claims_enriched_output,
        run_dq_checks,
        get_logger,
        get_run_id,
    )

    # Use functions in your own Spark session / pipeline
    # Or run the pipeline via: python -m claims_etl.main --help
"""

from typing import Final

# ------------------------------
# Version discovery (robust to dev vs installed environments)
# ------------------------------
try:
    # Python 3.8+: importlib.metadata is in stdlib; fallback to backport if needed.
    try:
        from importlib.metadata import PackageNotFoundError, version  # type: ignore
    except Exception:  # pragma: no cover - defensive fallback
        from importlib_metadata import PackageNotFoundError, version  # type: ignore

    # Try both plausible distribution names; fall back if not installed as a dist.
    try:
        __version__ = version("claims-etl")  # when packaged with hyphen
    except PackageNotFoundError:
        try:
            __version__ = version("claims_etl")  # when packaged with underscore
        except PackageNotFoundError:
            __version__ = "1.0.0"  # local/dev default
except Exception:  # pragma: no cover - extremely defensive
    __version__ = "1.0.0"


# ------------------------------
# Public API exports
# ------------------------------
from src.claims_etl.main import main as cli_main
from src.claims_etl.claims_etl_transforms import (
    clean_claims,
    normalize_policyholders,
    join_claims_policyholders,
    build_claims_enriched_output)

from src.claims_etl.claims_etl_dq import run_dq_checks, DQRuleResult
from src.claims_etl.claims_etl_utils import get_logger, get_run_id

__all__: Final[list[str]] = [
    # Version / metadata
    "__version__",
    # CLI
    "cli_main",
    # Core transforms
    "clean_claims",
    "normalize_policyholders",
    "join_claims_policyholders",
    "build_claims_enriched_output",
    # Data Quality
    "run_dq_checks",
    "DQRuleResult",
    # Logging / helpers
    "get_logger",
    "get_run_id",
]