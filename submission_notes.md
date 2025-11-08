# Submission Notes

## Dependency justification
- pandas, openpyxl: read JSON and Excel field mapping.
- SQLAlchemy, pymysql: connect and write to MySQL with safe parameterized queries.
- pydantic: validate incoming record shapes.
- python-dotenv: load DB credentials from an `.env` file.
- tqdm: progress bar for ETL operations.
- mysql-connector-python: included as an alternative connector.

## Design decisions
- Normalized logical groups: property core, property_detail, valuation, hoa, rehab_estimate, and property_attribute for sparse/misc attributes.
- The ETL attempts to map fields using `Field Config.xlsx`. If a mapping is not found, the field is stored in `property_attribute` as key/value.
- Rehab estimate breakdown is stored as JSON when present.

