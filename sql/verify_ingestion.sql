-- Run in DuckDB after: python scripts/ingest.py to verify all tables. 
-- Attach: duckdb database/superbadd.duckdb

-- 1) Row counts per table (catches empty loads or missing files).
-- Rivers
UNION ALL SELECT 'basinatlas', COUNT(*) FROM basinatlas
UNION ALL SELECT 'riveratlas', COUNT(*) FROM riveratlas
UNION ALL SELECT 'ffr', COUNT(*) FROM ffr;
-- Dams
SELECT 'gdw' AS tbl, COUNT(*) AS n FROM gdw
SELECT 'fhred' AS tbl, COUNT(*) AS n FROM fhred;
SELECT 'icold' AS tbl, COUNT(*) AS n FROM icold;

-- 2) Join smoke test: dam and reach share hyriv_id, so river_name should fill in.
SELECT g.dam_name, r.river_name
FROM gdw AS g
JOIN riveratlas AS r ON g.hyriv_id = r.hyriv_id
WHERE g.dam_name IS NOT NULL
LIMIT 5;
