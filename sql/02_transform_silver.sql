-- ============================================================================
-- Connecticut Real Estate: Bronze to Silver Transformation
-- ============================================================================
-- Description: Transforms raw data into validated, typed data
-- Author: Dokleat Halilaj
-- Run Order: 2 (After 01_setup_database.sql and data ingestion)
-- Dependencies: BRONZE.raw_sales must contain data
-- ============================================================================

USE DATABASE CT_REAL_ESTATE;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA SILVER;

-- ============================================================================
-- SECTION 1: Data Transformation
-- ============================================================================

-- Clear existing data (or use MERGE for incremental loads)
TRUNCATE TABLE sales_validated;

-- Transform and validate raw data
INSERT INTO sales_validated
SELECT
    -- ========================================================================
    -- Core Identifiers
    -- ========================================================================
    serial_number,
    
    -- ========================================================================
    -- Temporal Fields (Parse and Type Conversion)
    -- ========================================================================
    TRY_CAST(list_year AS INTEGER) AS list_year,
    TRY_TO_DATE(date_recorded, 'MM/DD/YYYY') AS date_recorded,
    
    -- Derived temporal fields
    YEAR(TRY_TO_DATE(date_recorded, 'MM/DD/YYYY')) AS sale_year,
    MONTH(TRY_TO_DATE(date_recorded, 'MM/DD/YYYY')) AS sale_month,
    QUARTER(TRY_TO_DATE(date_recorded, 'MM/DD/YYYY')) AS sale_quarter,
    
    -- ========================================================================
    -- Location Information (Standardize)
    -- ========================================================================
    UPPER(TRIM(town)) AS town,
    TRIM(address) AS address,
    
    -- ========================================================================
    -- Financial Fields (Clean and Convert)
    -- ========================================================================
    -- Remove commas and convert to number
    TRY_CAST(REPLACE(assessed_value, ',', '') AS NUMBER(12,2)) AS assessed_value,
    TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER(12,2)) AS sale_amount,
    TRY_CAST(sales_ratio AS FLOAT) AS sales_ratio,
    
    -- Calculated field: price per ratio point
    CASE 
        WHEN TRY_CAST(sales_ratio AS FLOAT) > 0 
        THEN TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER(12,2)) / 
             TRY_CAST(sales_ratio AS FLOAT)
        ELSE NULL 
    END AS price_per_ratio,
    
    -- ========================================================================
    -- Property Classification (Standardize)
    -- ========================================================================
    UPPER(TRIM(property_type)) AS property_type,
    UPPER(TRIM(residential_type)) AS residential_type,
    
    -- ========================================================================
    -- Additional Metadata
    -- ========================================================================
    non_use_code,
    assessor_remarks,
    opm_remarks,
    location,
    
    -- ========================================================================
    -- Data Quality Indicators
    -- ========================================================================
    
    -- is_valid_sale: Basic validation checks
    CASE
        WHEN TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER) >= 2000
            AND TRY_TO_DATE(date_recorded, 'MM/DD/YYYY') IS NOT NULL
            AND town IS NOT NULL
            AND TRIM(town) != ''
        THEN TRUE
        ELSE FALSE
    END AS is_valid_sale,
    
    -- is_arms_length: Check for non-use code (indicates family transfer, etc.)
    CASE
        WHEN non_use_code IS NULL OR TRIM(non_use_code) = ''
        THEN TRUE
        ELSE FALSE
    END AS is_arms_length,
    
    -- data_quality_score: Composite score (0-100)
    (
        -- Valid sale amount (≥ $2,000)
        CASE WHEN TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER) >= 2000 
             THEN 25 ELSE 0 END +
        
        -- Valid date
        CASE WHEN TRY_TO_DATE(date_recorded, 'MM/DD/YYYY') IS NOT NULL 
             THEN 25 ELSE 0 END +
        
        -- Valid town
        CASE WHEN town IS NOT NULL AND TRIM(town) != '' 
             THEN 25 ELSE 0 END +
        
        -- Valid address
        CASE WHEN address IS NOT NULL AND TRIM(address) != '' 
             THEN 25 ELSE 0 END
    ) AS data_quality_score,
    
    -- ========================================================================
    -- Audit Column
    -- ========================================================================
    CURRENT_TIMESTAMP() AS processed_timestamp
    
FROM CT_REAL_ESTATE.BRONZE.raw_sales
WHERE serial_number IS NOT NULL;

-- ============================================================================
-- SECTION 2: Data Quality Report
-- ============================================================================

-- Summary statistics
SELECT 
    '=== SILVER TRANSFORMATION SUMMARY ===' AS report_section;

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_valid_sale THEN 1 ELSE 0 END) AS valid_sales,
    SUM(CASE WHEN is_arms_length THEN 1 ELSE 0 END) AS arms_length_sales,
    ROUND(AVG(data_quality_score), 2) AS avg_quality_score,
    MIN(date_recorded) AS earliest_sale,
    MAX(date_recorded) AS latest_sale
FROM sales_validated;

-- Data quality distribution
SELECT 
    '=== DATA QUALITY DISTRIBUTION ===' AS report_section;

SELECT
    CASE 
        WHEN data_quality_score = 100 THEN 'Excellent (100)'
        WHEN data_quality_score >= 75 THEN 'Good (75-99)'
        WHEN data_quality_score >= 50 THEN 'Fair (50-74)'
        ELSE 'Poor (<50)'
    END AS quality_tier,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM sales_validated
GROUP BY quality_tier
ORDER BY MIN(data_quality_score) DESC;

-- Invalid records analysis
SELECT 
    '=== INVALID RECORDS ANALYSIS ===' AS report_section;

SELECT
    CASE 
        WHEN sale_amount < 2000 THEN 'Sale amount too low'
        WHEN date_recorded IS NULL THEN 'Missing date'
        WHEN town IS NULL OR town = '' THEN 'Missing town'
        ELSE 'Other'
    END AS invalid_reason,
    COUNT(*) AS count
FROM sales_validated
WHERE is_valid_sale = FALSE
GROUP BY invalid_reason
ORDER BY count DESC;

-- Top towns by volume
SELECT 
    '=== TOP 10 TOWNS BY SALES VOLUME ===' AS report_section;

SELECT
    town,
    COUNT(*) AS total_sales,
    ROUND(AVG(sale_amount), 0) AS avg_price,
    ROUND(SUM(sale_amount) / 1000000, 2) AS total_volume_millions
FROM sales_validated
WHERE is_valid_sale = TRUE
GROUP BY town
ORDER BY total_sales DESC
LIMIT 10;

-- ============================================================================
-- SECTION 3: Create Stored Procedure for Future Runs
-- ============================================================================

CREATE OR REPLACE PROCEDURE transform_to_silver()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Transform Bronze data to Silver layer'
AS
$$
BEGIN
    -- Truncate Silver table
    TRUNCATE TABLE CT_REAL_ESTATE.SILVER.sales_validated;
    
    -- Insert transformed data (same logic as above)
    INSERT INTO CT_REAL_ESTATE.SILVER.sales_validated
    SELECT
        serial_number,
        TRY_CAST(list_year AS INTEGER),
        TRY_TO_DATE(date_recorded, 'MM/DD/YYYY'),
        YEAR(TRY_TO_DATE(date_recorded, 'MM/DD/YYYY')),
        MONTH(TRY_TO_DATE(date_recorded, 'MM/DD/YYYY')),
        QUARTER(TRY_TO_DATE(date_recorded, 'MM/DD/YYYY')),
        UPPER(TRIM(town)),
        TRIM(address),
        TRY_CAST(REPLACE(assessed_value, ',', '') AS NUMBER(12,2)),
        TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER(12,2)),
        TRY_CAST(sales_ratio AS FLOAT),
        CASE WHEN TRY_CAST(sales_ratio AS FLOAT) > 0 
             THEN TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER(12,2)) / TRY_CAST(sales_ratio AS FLOAT)
             ELSE NULL END,
        UPPER(TRIM(property_type)),
        UPPER(TRIM(residential_type)),
        non_use_code,
        assessor_remarks,
        opm_remarks,
        location,
        CASE WHEN TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER) >= 2000
                  AND TRY_TO_DATE(date_recorded, 'MM/DD/YYYY') IS NOT NULL
                  AND town IS NOT NULL
             THEN TRUE ELSE FALSE END,
        CASE WHEN non_use_code IS NULL OR TRIM(non_use_code) = ''
             THEN TRUE ELSE FALSE END,
        (CASE WHEN TRY_CAST(REPLACE(sale_amount, ',', '') AS NUMBER) >= 2000 THEN 25 ELSE 0 END +
         CASE WHEN TRY_TO_DATE(date_recorded, 'MM/DD/YYYY') IS NOT NULL THEN 25 ELSE 0 END +
         CASE WHEN town IS NOT NULL AND TRIM(town) != '' THEN 25 ELSE 0 END +
         CASE WHEN address IS NOT NULL AND TRIM(address) != '' THEN 25 ELSE 0 END),
        CURRENT_TIMESTAMP()
    FROM CT_REAL_ESTATE.BRONZE.raw_sales
    WHERE serial_number IS NOT NULL;
    
    -- Return success message
    RETURN 'Silver transformation completed: ' || 
           (SELECT COUNT(*) FROM CT_REAL_ESTATE.SILVER.sales_validated) || 
           ' records processed';
END;
$$;

-- ============================================================================
-- SECTION 4: Completion Message
-- ============================================================================

SELECT 
    '✓ Silver transformation complete!' AS status,
    (SELECT COUNT(*) FROM sales_validated) AS total_records,
    (SELECT COUNT(*) FROM sales_validated WHERE is_valid_sale = TRUE) AS valid_records;

-- ============================================================================
-- End of Transformation Script
-- ============================================================================
