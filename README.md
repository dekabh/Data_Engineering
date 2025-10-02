## How to run
# Install python 3.10 compatible with Spark 4.0.1
# 1) Create a virtual environment (recommended)
python -m venv .venv

# Linux/macOS:
source .venv/bin/activate
# Windows (PowerShell):
# Execute & .\.venv\Scripts\Activate.ps1

# 2) Install dependencies
pip install -r .\pyspark-claims-etl\requirements.txt

# 3) Run tests
pytest -q

# 4) Run the pipeline (seeds the provided CSVs if missing)
python ./pyspark-claims-etl/src/claims_etl/main.py --input-dir './data/input/' --output-dir './data/output/' --mode 'overwrite' --fail-on-dq-error 'true' --log-dir './data/output/logs/'

###################
  # PySpark Claims ETL (DQ + Logging)

This project demonstrates a production-ready PySpark ETL for insurance claims:
- Reads **claims** & **policyholders** from CSV
- Cleans & normalizes data
- Runs **data quality checks** (uniqueness, domain, validity, referential integrity, consistency)
- Writes **Gold** (policyholder monthly aggregates)
- Emits **JSON DQ report** and **structured logs** (console + file)

## Quickstart


python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -q

python ./pyspark-claims-etl/src/claims_etl/main.py --input-dir './data/input/' --output-dir './data/output/' --mode 'overwrite' --fail-on-dq-error 'true' --log-dir './data/output/logs/'
