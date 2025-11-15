-- ============================================================================
-- SECTION 1: Database and Schema Creation
-- ============================================================================

CREATE DATABASE IF NOT EXISTS CT_REAL_ESTATE
    COMMENT = 'Connecticut Real Estate Sales Data (2001-2023)';

USE DATABASE CT_REAL_ESTATE;

-- Create schemas for Medallion Architecture
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw data layer - exact copy from source';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Validated and cleaned data layer';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Business analytics and aggregated metrics';

-- ============================================================================
-- SECTION 2: Compute Warehouse Setup
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60                -- Suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE               -- Auto-resume when queries submitted
    INITIALLY_SUSPENDED = TRUE       -- Start suspended to save costs
    COMMENT = 'Warehouse for CT Real Estate ETL pipeline';

USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- SECTION 3: BRONZE LAYER - Raw Data Tables
-- ============================================================================

USE SCHEMA BRONZE;

-- Main raw sales table
CREATE OR REPLACE TABLE raw_sales (
    -- Core identifiers
    serial_number STRING COMMENT 'Unique sale identifier from CT system',
    
    -- Temporal fields (stored as strings initially)
    list_year STRING COMMENT 'Grand list year (fiscal year)',
    date_recorded STRING COMMENT 'Sale recording date',
    
    -- Location information
    town STRING COMMENT 'Connecticut municipality',
    address STRING COMMENT 'Property address',
    
    -- Financial fields (stored as strings to preserve original format)
    assessed_value STRING COMMENT 'Town-assessed property value',
    sale_amount STRING COMMENT 'Actual sale price',
    sales_ratio STRING COMMENT 'Assessment to sale ratio',
    
    -- Property classification
    property_type STRING COMMENT 'Property category (Residential, Commercial, etc.)',
    residential_type STRING COMMENT 'Residential sub-type (Single Family, Condo, etc.)',
    
    -- Additional metadata
    non_use_code STRING COMMENT 'Code indicating non-arms-length transaction',
    assessor_remarks STRING COMMENT 'Comments from local assessor',
    opm_remarks STRING COMMENT 'Comments from Office of Policy and Management',
    location STRING COMMENT 'Geographic coordinates',
    
    -- Audit columns
    load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'ETL load timestamp',
    source_file STRING DEFAULT 'CT_OPEN_DATA' COMMENT 'Data source identifier'
)
COMMENT = 'Raw real estate sales data from Connecticut Open Data Portal';

-- ============================================================================
-- SECTION 4: SILVER LAYER - Validated Data Tables
-- ============================================================================

USE SCHEMA SILVER;

-- Validated and typed sales table
CREATE OR REPLACE TABLE sales_validated (
    -- Core identifiers
    serial_number STRING PRIMARY KEY,
    
    -- Temporal fields (properly typed)
    list_year INTEGER COMMENT 'Grand list year as integer',
    date_recorded DATE COMMENT 'Sale recording date',
    sale_year INTEGER COMMENT 'Year of sale (derived)',
    sale_month INTEGER COMMENT 'Month of sale (derived)',
    sale_quarter INTEGER COMMENT 'Quarter of sale (derived)',
    
    -- Location information (cleaned)
    town STRING COMMENT 'Town name (uppercase, trimmed)',
    address STRING COMMENT 'Property address (trimmed)',
    
    -- Financial fields (properly typed)
    assessed_value NUMBER(12,2) COMMENT 'Assessed value in dollars',
    sale_amount NUMBER(12,2) COMMENT 'Sale price in dollars',
    sales_ratio FLOAT COMMENT 'Assessment/Sale ratio',
    price_per_ratio FLOAT COMMENT 'Calculated price per ratio point',
    
    -- Property classification (standardized)
    property_type STRING COMMENT 'Property type (standardized)',
    residential_type STRING COMMENT 'Residential type (standardized)',
    
    -- Additional metadata
    non_use_code STRING COMMENT 'Non-use code',
    assessor_remarks STRING COMMENT 'Assessor remarks',
    opm_remarks STRING COMMENT 'OPM remarks',
    location STRING COMMENT 'Geographic coordinates',
    
    -- Data quality indicators
    is_valid_sale BOOLEAN COMMENT 'Meets validation criteria',
    is_arms_length BOOLEAN COMMENT 'Arms-length transaction indicator',
    data_quality_score INTEGER COMMENT 'Quality score (0-100)',
    
    -- Audit column
    processed_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Processing timestamp'
)
COMMENT = 'Validated and cleaned real estate sales data';

-- Create stream for change data capture
CREATE OR REPLACE STREAM sales_stream 
ON TABLE CT_REAL_ESTATE.BRONZE.raw_sales
COMMENT = 'CDC stream for bronze raw_sales table';

-- ============================================================================
-- SECTION 5: GOLD LAYER - Analytics Tables
-- ============================================================================

USE SCHEMA GOLD;

-- Town-level yearly statistics
CREATE OR REPLACE TABLE town_yearly_stats (
    town STRING NOT NULL,
    year INTEGER NOT NULL,
    total_sales INTEGER COMMENT 'Number of sales in the year',
    total_volume NUMBER(18,2) COMMENT 'Total dollar volume',
    avg_sale_price NUMBER(12,2) COMMENT 'Average sale price',
    median_sale_price NUMBER(12,2) COMMENT 'Median sale price',
    min_sale_price NUMBER(12,2) COMMENT 'Minimum sale price',
    max_sale_price NUMBER(12,2) COMMENT 'Maximum sale price',
    avg_assessed_value NUMBER(12,2) COMMENT 'Average assessed value',
    avg_sales_ratio FLOAT COMMENT 'Average sales ratio',
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    PRIMARY KEY (town, year)
)
COMMENT = 'Annual statistics aggregated by town';

-- Property type trends over time
CREATE OR REPLACE TABLE property_type_trends (
    property_type STRING NOT NULL,
    residential_type STRING NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    total_sales INTEGER COMMENT 'Number of sales in quarter',
    avg_price NUMBER(12,2) COMMENT 'Average sale price',
    median_price NUMBER(12,2) COMMENT 'Median sale price',
    price_growth_yoy FLOAT COMMENT 'Year-over-year price growth %',
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    PRIMARY KEY (property_type, residential_type, year, quarter)
)
COMMENT = 'Quarterly property type trends with YoY growth';

-- Market summary metrics
CREATE OR REPLACE TABLE market_summary (
    metric_name STRING PRIMARY KEY,
    metric_value VARIANT COMMENT 'Flexible value storage (number, string, object)',
    metric_date DATE COMMENT 'Relevant date for metric',
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
)
COMMENT = 'Key market metrics and indicators';

-- Monthly price index
CREATE OR REPLACE TABLE monthly_price_index (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    property_type STRING NOT NULL,
    avg_price NUMBER(12,2) COMMENT 'Average price for the month',
    sales_volume INTEGER COMMENT 'Number of sales',
    price_index FLOAT COMMENT 'Price index (base year = 100)',
    mom_change FLOAT COMMENT 'Month-over-month % change',
    yoy_change FLOAT COMMENT 'Year-over-year % change',
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    PRIMARY KEY (year, month, property_type)
)
COMMENT = 'Monthly price index with MoM and YoY changes';

-- ============================================================================
-- SECTION 6: File Formats
-- ============================================================================

USE SCHEMA BRONZE;

-- CSV file format for loading
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    EMPTY_FIELD_AS_NULL = TRUE
    COMMENT = 'Standard CSV format for CT data';

-- ============================================================================
-- SECTION 7: Verification
-- ============================================================================

-- Verify setup
SELECT 
    'Database setup complete!' AS status,
    CURRENT_DATABASE() AS database,
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_USER() AS user;

-- Show created schemas
SHOW SCHEMAS IN DATABASE CT_REAL_ESTATE;

-- Show created tables
SHOW TABLES IN SCHEMA CT_REAL_ESTATE.BRONZE;
SHOW TABLES IN SCHEMA CT_REAL_ESTATE.SILVER;
SHOW TABLES IN SCHEMA CT_REAL_ESTATE.GOLD;
