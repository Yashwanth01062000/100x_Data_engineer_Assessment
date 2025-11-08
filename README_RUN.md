# How to run the ETL (README)

Prerequisites
- Docker & docker-compose (provided in the assessment skeleton)
- Python 3.8+
- The assessment's `docker-compose.initial.yml` should be used to start MySQL as instructed in the assignment.

Steps
1. Start MySQL using the provided docker-compose:
   ```bash
   docker-compose -f docker-compose.initial.yml up --build -d
   ```
   Database will be available at `localhost:3306` with credentials defined in the compose file.

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # On Windows use: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Copy DB credentials into `.env` file based on `.env.template` or ensure they match the docker compose file. Example `.env`:
   ```env
   DB_USER=root
   DB_PASSWORD=root
   DB_HOST=127.0.0.1
   DB_PORT=3306
   DB_NAME=assessment_db
   ```

4. Place the provided `properties.json` and `Field Config.xlsx` into the `data/` folder in this repository.

5. (Optional) Create schema in MySQL beforehand:
   ```bash
   mysql -h 127.0.0.1 -u root -p < sql/schema.sql
   ```
   Or allow the ETL script to create tables if missing.

6. Run the ETL:
   ```bash
   python src/etl.py --json data/properties.json --excel "data/Field Config.xlsx" --env .env
   ```

7. Verify results by connecting to MySQL and querying tables like `property`, `property_detail`, `hoa`, `valuation`.
