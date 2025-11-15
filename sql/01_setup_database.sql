-- Connecticut Real Estate: Database Setup
-- Run this first to create all necessary database objects

CREATE DATABASE IF NOT EXISTS CT_REAL_ESTATE;
USE DATABASE CT_REAL_ESTATE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH 
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Continue with table creation...
-- (Copy nga Step 3.2 më lartë)