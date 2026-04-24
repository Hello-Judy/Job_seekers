-- ============================================================
-- 01: Create database, warehouse, and schemas
-- Run this FIRST in the Snowflake UI (Worksheets).
-- ============================================================

-- Use the default account-level role
USE ROLE ACCOUNTADMIN;

-- Warehouse: tiny one is enough for this project
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS JOBSEEKERS;

USE DATABASE JOBSEEKERS;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Sanity check
SHOW SCHEMAS IN DATABASE JOBSEEKERS;
