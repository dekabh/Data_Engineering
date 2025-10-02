from __future__ import annotations

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DecimalType,
)

# Input claims CSV uses 'claim_urgency' instead of 'claim_type'.
CLAIMS_INPUT_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), True),
        StructField("policyholder_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("claim_urgency", StringType(), True),
        StructField("claim_amount", StringType(), True),
        StructField("claim_date", StringType(), True),
    ]
)

POLICYHOLDER_SCHEMA = StructType(
    [
        StructField("policyholder_id", StringType(), False),
        StructField("policyholder_name", StringType(), True),
        StructField("region", StringType(), True),
    ]
)

# Silver (curated) claims schema
CLAIMS_SILVER_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), False),
        StructField("policyholder_id", StringType(), False),
        StructField("region", StringType(), False),             # Title case (East/West/...)
        StructField("claim_type", StringType(), True),          # optional in source
        StructField("claim_priority", StringType(), True),      # from claim_urgency        
        StructField("claim_amount", DecimalType(18, 2), False),
        StructField("claim_date", DateType(), False),
    ]
)