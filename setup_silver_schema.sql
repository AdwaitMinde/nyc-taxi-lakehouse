-- run once before the silver notebooks

USE CATALOG nyc_taxi;
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'silver layer, cleaned + deduped + validated';
