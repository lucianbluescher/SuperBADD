-- =============================================================================
-- SuperBADD Data Ingestion
-- =============================================================================

CREATE TABLE basinatlas AS 
SELECT * FROM read_csv_auto('data/clean/basinatlas.csv');

CREATE TABLE riveratlas AS 
SELECT * FROM read_csv_auto('data/clean/riveratlas.csv');

CREATE TABLE ffr AS 
SELECT * FROM read_csv_auto('data/clean/ffr.csv');

CREATE TABLE gdw AS
SELECT * FROM read_csv_auto('data/clean/gdw.csv');

--- not yet working
CREATE TABLE fhred AS 
SELECT * FROM read_csv_auto('data/clean/fhred.csv');
-- =============================================================================
-- Verification & Exploration
-- =============================================================================

SHOW TABLES;

SELECT COUNT(*) FROM gdw;

-- Check row counts for all tables to ensure full ingestion
SELECT 'gdw' AS table_name, COUNT(*) AS row_count FROM gdw
UNION ALL
SELECT 'basinatlas', COUNT(*) FROM basinatlas
UNION ALL
SELECT 'riveratlas', COUNT(*) FROM riveratlas
UNION ALL
SELECT 'ffr', COUNT(*) FROM ffr;

-- Inspect column types and nullability for the Dam Watch data
DESCRIBE gdw;


-- Find 10 largest dams to verify numeric sorting and data integrity
SELECT 
    dam_name, 
    country, 
    cap_mcm 
FROM gdw 
WHERE cap_mcm IS NOT NULL
ORDER BY cap_mcm DESC 
LIMIT 10;

-- Test a JOIN between GDW and FFR (River Network) 
-- to ensure the hyriv_id keys are matching correctly
SELECT 
    g.dam_name, 
    f.hyriv_id,
    f.river_name
FROM gdw g
JOIN ffr f ON g.hyriv_id = f.hyriv_id
LIMIT 5;


-- Spatial extension helps DuckDB understand the geometry columns in your Parquet files
-- INSTALL spatial;
-- LOAD spatial;
