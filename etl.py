#!/usr/bin/env python3
"""ETL script to normalize raw property JSON into MySQL normalized schema."""

import argparse
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd
from pydantic import BaseModel, ValidationError, validator
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, BigInteger, String, Numeric, Boolean, Date, Text, JSON
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm
from dotenv import load_dotenv


class PropertyModel(BaseModel):
    external_id: Optional[str]
    address_line1: Optional[str]
    address_line2: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    county: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

    bedrooms: Optional[int]
    bathrooms: Optional[float]
    sqft: Optional[int]
    year_built: Optional[int]
    property_type: Optional[str]

    has_hoa: Optional[bool]
    hoa_name: Optional[str]
    hoa_fee_amount: Optional[float]
    hoa_fee_frequency: Optional[str]

    valuation_source: Optional[str]
    valuation_amount: Optional[float]
    valuation_date: Optional[datetime]

    rehab_estimate_total: Optional[float]
    rehab_estimate_breakdown: Optional[Dict[str, Any]]

    other_attributes: Optional[Dict[str, Any]] = {}

    @validator('postal_code')
    def postal_code_strip(cls, v):
        if v is None:
            return v
        return str(v).strip()


def load_field_config(excel_path: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, engine='openpyxl')
    df.columns = [c.strip() for c in df.columns]
    return df


def build_engine_from_env(env_path: Optional[str] = None):
    if env_path and os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        load_dotenv()
    user = os.getenv('DB_USER', 'root')
    pw = os.getenv('DB_PASSWORD', 'root')
    host = os.getenv('DB_HOST', '127.0.0.1')
    port = os.getenv('DB_PORT', '3306')
    db = os.getenv('DB_NAME', 'assessment_db')
    engine_url = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}?charset=utf8mb4"
    engine = create_engine(engine_url, echo=False, future=True)
    return engine


def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS property (
            property_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            external_id VARCHAR(128) UNIQUE,
            address_line1 VARCHAR(255),
            address_line2 VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code VARCHAR(20),
            county VARCHAR(100),
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS property_detail (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            property_id BIGINT NOT NULL,
            bedrooms INT,
            bathrooms DECIMAL(3,1),
            sqft INT,
            year_built INT,
            property_type VARCHAR(100),
            zoning VARCHAR(50)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS valuation (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            property_id BIGINT NOT NULL,
            valuation_source VARCHAR(100),
            valuation_amount DECIMAL(15,2),
            valuation_date DATE,
            notes TEXT
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS hoa (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            property_id BIGINT NOT NULL,
            has_hoa BOOLEAN,
            hoa_name VARCHAR(255),
            hoa_fee_amount DECIMAL(12,2),
            hoa_fee_frequency VARCHAR(50)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS rehab_estimate (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            property_id BIGINT NOT NULL,
            estimate_total DECIMAL(15,2),
            estimate_breakdown JSON,
            last_updated DATE
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS property_attribute (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            property_id BIGINT NOT NULL,
            attr_key VARCHAR(200),
            attr_value TEXT
        )""")


def map_row_to_model(row: Dict[str, Any], field_config: pd.DataFrame) -> PropertyModel:
    # Build a dict for PropertyModel based on field_config mapping if present.
    mapped = {}
    other = {}
    for k, v in row.items():
        # find mapping in field_config
        matches = field_config[field_config['raw_field'].astype(str).str.lower() == k.lower()]
        if not matches.empty:
            target = matches.iloc[0]['target_column']
            tbl = matches.iloc[0]['target_table']
            key = str(target).strip()
            # map based on simple rules we expect
            mapped[key] = v
        else:
            other[k] = v
    mapped['other_attributes'] = other
    try:
        pm = PropertyModel(**mapped)
    except ValidationError as e:
        raise e
    return pm


def insert_property(conn, engine, pm: PropertyModel):
    # Insert into property table and related tables. Return property_id.
    with engine.begin() as conn:
        # property core
        prop_ins = insert(conn.table_names) if False else None  # placeholder to keep linter quiet
    # We'll use raw SQL for simplicity
    with engine.connect() as conn:
        res = conn.execute(
            """INSERT INTO property (external_id,address_line1,address_line2,city,state,postal_code,county,latitude,longitude)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (pm.external_id, pm.address_line1, pm.address_line2, pm.city, pm.state, pm.postal_code, pm.county,
             pm.latitude, pm.longitude)
        )
        # Get last insert id
        prop_id = conn.execute("SELECT LAST_INSERT_ID()").scalar()
        # property_detail
        conn.execute(
            """INSERT INTO property_detail (property_id,bedrooms,bathrooms,sqft,year_built,property_type,zoning)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (prop_id, pm.bedrooms, pm.bathrooms, pm.sqft, pm.year_built, pm.property_type, None)
        )
        # valuation
        if pm.valuation_amount is not None or pm.valuation_source is not None:
            val_date = pm.valuation_date.date() if pm.valuation_date else None
            conn.execute(
                """INSERT INTO valuation (property_id,valuation_source,valuation_amount,valuation_date,notes)
                   VALUES (%s,%s,%s,%s,%s)""",
                (prop_id, pm.valuation_source, pm.valuation_amount, val_date, None)
            )
        # hoa
        if pm.has_hoa is not None or pm.hoa_name is not None:
            conn.execute(
                """INSERT INTO hoa (property_id,has_hoa,hoa_name,hoa_fee_amount,hoa_fee_frequency)
                   VALUES (%s,%s,%s,%s,%s)""",
                (prop_id, pm.has_hoa, pm.hoa_name, pm.hoa_fee_amount, pm.hoa_fee_frequency)
            )
        # rehab_estimate
        if pm.rehab_estimate_total is not None or pm.rehab_estimate_breakdown:
            breakdown_json = json.dumps(pm.rehab_estimate_breakdown) if pm.rehab_estimate_breakdown else None
            conn.execute(
                """INSERT INTO rehab_estimate (property_id,estimate_total,estimate_breakdown,last_updated)
                   VALUES (%s,%s,%s,%s)""",
                (prop_id, pm.rehab_estimate_total, breakdown_json, None)
            )
        # other attributes
        for k, v in (pm.other_attributes or {}).items():
            conn.execute(
                """INSERT INTO property_attribute (property_id,attr_key,attr_value)
                   VALUES (%s,%s,%s)""",
                (prop_id, str(k), json.dumps(v) if isinstance(v, (dict, list)) else str(v))
            )
    return prop_id


def main(args):
    field_config = load_field_config(args.excel)
    engine = build_engine_from_env(args.env)
    ensure_tables(engine)

    # Load JSON
    df = pd.read_json(args.json, lines=True)
    print(f"Loaded {len(df)} records from {args.json}")
    failed = 0

    for _, row in tqdm(df.iterrows(), total=len(df), desc='Processing'):
        try:
            pm = map_row_to_model(row.to_dict(), field_config)
            insert_property(None, engine, pm)
        except Exception as e:
            failed += 1
            print(f"Failed to process row due to: {e}")
    print(f"Completed. Failed rows: {failed}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ETL for property JSON')
    parser.add_argument('--json', required=True, help='Path to properties.json')
    parser.add_argument('--excel', required=True, help='Path to Field Config.xlsx')
    parser.add_argument('--env', default='.env', help='Path to .env file')
    args = parser.parse_args()
    main(args)
