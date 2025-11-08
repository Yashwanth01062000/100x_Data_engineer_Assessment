# Data Engineering Assessment — Submission by Yashwanth Katuru

This archive contains the files required to reproduce the normalized schema and ETL for the assessment.

## Included files
- `requirements.txt` — Python dependencies.
- `README_RUN.md` — How to run the ETL and integrate with the provided Dockerized MySQL.
- `sql/schema.sql` — DDL to create the normalized schema.
- `src/etl.py` — Python ETL script that reads `data/properties.json` and `data/Field Config.xlsx` and loads into MySQL.
- `.env.template` — Template for DB credentials (do NOT commit real credentials).
- `submission_notes.md` — Short notes and justification for dependencies and design decisions.

Place the original `properties.json` and `Field Config.xlsx` into the `data/` folder before running the script.
