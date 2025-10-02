#!/.venv/bin/python
from __future__ import annotations

import json
import logging
import sys
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, DataFrame


def get_logger(name: str, log_dir: str | Path | None = None, run_id: str | None = None) -> logging.Logger:
    """
    Configure a structured logger with console + rotating file handlers.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.INFO)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | run_id=%(run_id)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # Console handler
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    logger.addHandler(ch)

    # File handler (rotating)
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(str(Path(log_dir) / "pipeline.log"), maxBytes=5_000_000, backupCount=3)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
        logger.addHandler(fh)

    # Inject run_id into all log records
    class ContextFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.run_id = run_id or "N/A"
            return True

    logger.addFilter(ContextFilter())

    return logger


def get_run_id() -> str:
    return str(uuid.uuid4())


def get_spark(app_name: str = "claims-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )


def safe_mkdir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def write_json(path: str | Path, obj: Any) -> None:
    p = Path(path)
    safe_mkdir(p.parent)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def log_df_info(log: logging.Logger, label: str, df: DataFrame) -> None:
    """
    Log row count and schema for a DataFrame, with protection against expensive counts in huge data.
    """
    try:
        cnt = df.count()
    except Exception as e:
        cnt = f"count_failed: {e!r}"
    schema = df._jdf.schema().treeString() if hasattr(df, "_jdf") else str(df.schema)
    log.info("DF[%s] rows=%s", label, cnt)
    log.info("DF[%s] schema:\n%s", label, schema)


def seed_sample_data(base_input_dir: str | Path) -> None:
    """
    Seeds input directory with the provided CSVs if absent.
    """
    base = Path(base_input_dir)
    claims_path = base / "claims_data.csv"
    policyholders_path = base / "policyholder_data.csv"

    if claims_path.exists() and policyholders_path.exists():
        return

    safe_mkdir(base)

    claims_csv = """claim_id,policyholder_id,region,claim_urgency,claim_amount,claim_date
                        CL_472,PH_101,East,High,1500.75,2023-01-15
                        RX_819,PH_101,West,Low,4200.00,2023-02-10
                        CX_235,PH_237,West,High,4500.50,2023-03-05
                        CL_627,PH_115,North,Low,2800.25,2023-04-20
                        RX_394,PH_237,South,High,1800.00,2023-05-12
                        CL_153,PH_152,South,Low,5100.00,2023-06-08
                    """
    policyholders_csv = """policyholder_id,policyholder_name,region
                            PH_101,John Doe,East
                            PH_237,Jane Smith,West
                            PH_287,Alice Brown,North
                            PH_152,Bob Johnson,South
                            """

    claims_path.write_text(claims_csv, encoding="utf-8")
    policyholders_path.write_text(policyholders_csv, encoding="utf-8")
