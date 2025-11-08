-- schema.sql
CREATE DATABASE IF NOT EXISTS assessment_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE assessment_db;

CREATE TABLE IF NOT EXISTS property (
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
);

CREATE TABLE IF NOT EXISTS property_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    bedrooms INT,
    bathrooms DECIMAL(3,1),
    sqft INT,
    year_built INT,
    property_type VARCHAR(100),
    zoning VARCHAR(50),
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS valuation (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    valuation_source VARCHAR(100),
    valuation_amount DECIMAL(15,2),
    valuation_date DATE,
    notes TEXT,
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS hoa (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    has_hoa BOOLEAN,
    hoa_name VARCHAR(255),
    hoa_fee_amount DECIMAL(12,2),
    hoa_fee_frequency VARCHAR(50),
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS rehab_estimate (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    estimate_total DECIMAL(15,2),
    estimate_breakdown JSON,
    last_updated DATE,
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS property_attribute (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    attr_key VARCHAR(200),
    attr_value TEXT,
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE,
    INDEX idx_attr_key (attr_key)
);
