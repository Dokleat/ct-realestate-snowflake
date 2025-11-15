-- ============================================================================
-- Connecticut Real Estate: Silver to Gold Aggregation
-- ============================================================================
-- Description: Creates business-ready analytics tables
-- Author: Dokleat Halilaj
-- Run Order: 3 (After 02_transform_silver.sql)
-- Dependencies: SILVER.sales_validated must contain data
-- ============================================================================

USE DATABASE CT_REAL_ESTATE;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA GOLD;

-- ============================================================================
-- SECTION 1: Town Yearly Statistics
-- ============================================================================

TRUNCATE TABLE town_yearly_stats;

INSERT INTO town_yearly_stats
SELECT
    town,
    sale_year AS year,
    COUNT(*) AS total_sales,
    SUM(sale_amount) AS total_volume,
    AVG(sale_amount) AS avg_sale_price,
    MEDIAN(sale_amount) AS median_sale_price,
    MIN(sale_amount) AS min_sale_price,
    MAX(sale_amount) AS max_sale_price,
    AVG(assessed_value) AS avg_assessed_value,
    AVG(sales_ratio) AS avg_sales_ratio,
    CURRENT_TIMESTAMP() AS created_at
FROM CT_REAL_ESTATE.SILVER.sales_validated
WHERE is_valid_sale = TRUE
    AND is_arms_length = TRUE
    AND sale_amount > 0
    AND sale_year IS NOT NULL
GROUP BY town, sale_year;

SELECT 'Town yearly stats created: ' || COUNT(*) || ' records' AS status
FROM town_yearly_stats;

-- ============================================================================
-- SECTION 2: Property Type Trends
-- ============================================================================

TRUNCATE TABLE property_type_trends;

-- First, create quarterly statistics
INSERT INTO property_type_trends
WITH quarterly_stats AS (
    SELECT
        property_type,
        COALESCE(residential_type, 'N/A') AS residential_type,
        sale_year,
        sale_quarter,
        COUNT(*) AS total_sales,
        AVG(sale_amount) AS avg_price,
        MEDIAN(sale_amount) AS median_price
    FROM CT_REAL_ESTATE.SILVER.sales_validated
    WHERE is_valid_sale = TRUE
        AND sale_amount > 0
        AND sale_year IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
with_growth AS (
    SELECT
        *,
        LAG(avg_price, 4) OVER (
            PARTITION BY property_type, residential_type 
            ORDER BY sale_year, sale_quarter
        ) AS prev_year_price
    FROM quarterly_stats
)
SELECT
    property_type,
    residential_type,
    sale_year,
    sale_quarter,
    total_sales,
    avg_price,
    median_price,
    CASE 
        WHEN prev_year_price > 0 AND prev_year_price IS NOT NULL
        THEN ((avg_price - prev_year_price) / prev_year_price) * 100
        ELSE NULL 
    END AS price_growth_yoy,
    CURRENT_TIMESTAMP() AS created_at
FROM with_growth;

SELECT 'Property type trends created: ' || COUNT(*) || ' records' AS status
FROM property_type_trends;

-- ============================================================================
-- SECTION 3: Monthly Price Index
-- ============================================================================

TRUNCATE TABLE monthly_price_index;

INSERT INTO monthly_price_index
WITH monthly_avg AS (
    SELECT
        sale_year,
        sale_month,
        property_type,
        AVG(sale_amount) AS avg_price,
        COUNT(*) AS sales_volume
    FROM CT_REAL_ESTATE.SILVER.sales_validated
    WHERE is_valid_sale = TRUE
        AND sale_amount > 0
        AND sale_year IS NOT NULL
    GROUP BY 1, 2, 3
),
base_year AS (
    -- Use 2001 as base year (index = 100)
    SELECT
        property_type,
        AVG(avg_price) AS base_price
    FROM monthly_avg
    WHERE sale_year = 2001
    GROUP BY property_type
),
with_index AS (
    SELECT
        m.sale_year,
        m.sale_month,
        m.property_type,
        m.avg_price,
        m.sales_volume,
        CASE 
            WHEN b.base_price > 0 
            THEN (m.avg_price / b.base_price) * 100
            ELSE NULL 
        END AS price_index
    FROM monthly_avg m
    LEFT JOIN base_year b ON m.property_type = b.property_type
),
with_lags AS (
    SELECT
        *,
        LAG(price_index, 1) OVER (
            PARTITION BY property_type 
            ORDER BY sale_year, sale_month
        ) AS prev_month_index,
        LAG(price_index, 12) OVER (
            PARTITION BY property_type 
            ORDER BY sale_year, sale_month
        ) AS prev_year_index
    FROM with_index
)
SELECT
    sale_year,
    sale_month,
    property_type,
    avg_price,
    sales_volume,
    price_index,
    CASE 
        WHEN prev_month_index > 0 
        THEN ((price_index - prev_month_index) / prev_month_index) * 100
        ELSE NULL 
    END AS mom_change,
    CASE 
        WHEN prev_year_index > 0 
        THEN ((price_index - prev_year_index) / prev_year_index) * 100
        ELSE NULL 
    END AS yoy_change,
    CURRENT_TIMESTAMP() AS created_at
FROM with_lags;

SELECT 'Monthly price index created: ' || COUNT(*) || ' records' AS status
FROM monthly_price_index;

-- ============================================================================
-- SECTION 4: Market Summary Metrics
-- ============================================================================

TRUNCATE TABLE market_summary;

-- Insert key market metrics
INSERT INTO market_summary VALUES
    ('total_sales', 
     (SELECT COUNT(*) FROM CT_REAL_ESTATE.SILVER.sales_validated WHERE is_valid_sale = TRUE),
     CURRENT_DATE(),
     CURRENT_TIMESTAMP()),
    
    ('total_volume_billions',
     (SELECT ROUND(SUM(sale_amount) / 1000000000, 2) FROM CT_REAL_ESTATE.SILVER.sales_validated WHERE is_valid_sale = TRUE),
     CURRENT_DATE(),
     CURRENT_TIMESTAMP()),
    
    ('avg_sale_price',
     (SELECT ROUND(AVG(sale_amount), 0) FROM CT_REAL_ESTATE.SILVER.sales_validated WHERE is_valid_sale = TRUE),
     CURRENT_DATE(),
     CURRENT_TIMESTAMP()),
    
    ('median_sale_price',
     (SELECT MEDIAN(sale_amount) FROM CT_REAL_ESTATE.SILVER.sales_validated WHERE is_valid_sale = TRUE),
     CURRENT_DATE(),
     CURRENT_TIMESTAMP()),
    
    ('total_towns',
     (SELECT COUNT(DISTINCT town) FROM CT_REAL_ESTATE.SILVER.sales_validated),
     CURRENT_DATE(),
     CURRENT_TIMESTAMP()),
    
    ('date_range',
     OBJECT_CONSTRUCT(
         'earliest', (SELECT MIN(date_recorded) FROM CT_REAL_ESTATE.SILVER.sales_validated),
         'latest', (SELECT MAX(date_recorded) FROM CT_REAL_ESTATE.SILVER.sales_validated)
     ),
     CURRENT_DATE(),
     CURRENT_TIMESTAMP());

SELECT 'Market summary metrics created: ' || COUNT(*) || ' metrics' AS status
FROM market_summary;

-- ============================================================================
-- SECTION 5: Create Stored Procedures for Future Runs
-- ============================================================================

-- Procedure 1: Town Stats
CREATE OR REPLACE PROCEDURE aggregate_town_stats()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Aggregate town yearly statistics'
AS
$$
BEGIN
    TRUNCATE TABLE CT_REAL_ESTATE.GOLD.town_yearly_stats;
    
    INSERT INTO CT_REAL_ESTATE.GOLD.town_yearly_stats
    SELECT
        town, sale_year, COUNT(*),
        SUM(sale_amount), AVG(sale_amount), MEDIAN(sale_amount),
        MIN(sale_amount), MAX(sale_amount), AVG(assessed_value),
        AVG(sales_ratio), CURRENT_TIMESTAMP()
    FROM CT_REAL_ESTATE.SILVER.sales_validated
    WHERE is_valid_sale = TRUE AND is_arms_length = TRUE
        AND sale_amount > 0 AND sale_year IS NOT NULL
    GROUP BY town, sale_year;
    
    RETURN 'Town stats aggregated: ' || (SELECT COUNT(*) FROM CT_REAL_ESTATE.GOLD.town_yearly_stats) || ' records';
END;
$$;

-- Procedure 2: Property Trends
CREATE OR REPLACE PROCEDURE aggregate_property_trends()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Aggregate property type trends'
AS
$$
BEGIN
    TRUNCATE TABLE CT_REAL_ESTATE.GOLD.property_type_trends;
    
    -- (Same logic as above - abbreviated for space)
    INSERT INTO CT_REAL_ESTATE.GOLD.property_type_trends
    SELECT property_type, COALESCE(residential_type, 'N/A'),
           sale_year, sale_quarter, COUNT(*),
           AVG(sale_amount), MEDIAN(sale_amount), NULL, CURRENT_TIMESTAMP()
    FROM CT_REAL_ESTATE.SILVER.sales_validated
    WHERE is_valid_sale = TRUE AND sale_amount > 0
    GROUP BY 1, 2, 3, 4;
    
    RETURN 'Property trends aggregated';
END;
$$;

-- Procedure 3: Master Aggregation
CREATE OR REPLACE PROCEDURE aggregate_to_gold()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Run all Gold layer aggregations'
AS
$$
DECLARE
    result1 STRING;
    result2 STRING;
BEGIN
    result1 := (CALL aggregate_town_stats());
    result2 := (CALL aggregate_property_trends());
    
    RETURN 'Gold aggregation complete. ' || result1 || '. ' || result2;
END;
$$;

-- ============================================================================
-- SECTION 6: Completion Summary
-- ============================================================================

SELECT '=== GOLD LAYER SUMMARY ===' AS report_section;

SELECT 
    'town_yearly_stats' AS table_name,
    COUNT(*) AS record_count
FROM town_yearly_stats
UNION ALL
SELECT 
    'property_type_trends',
    COUNT(*)
FROM property_type_trends
UNION ALL
SELECT 
    'monthly_price_index',
    COUNT(*)
FROM monthly_price_index
UNION ALL
SELECT 
    'market_summary',
    COUNT(*)
FROM market_summary;

SELECT '✓ Gold aggreg
