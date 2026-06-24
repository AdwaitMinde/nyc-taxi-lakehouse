-- run once before the gold notebooks

USE CATALOG nyc_taxi;
CREATE SCHEMA IF NOT EXISTS gold COMMENT 'gold layer, business aggregates';
