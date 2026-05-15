-- Run after: python scripts/ingest.py
-- Open DB: duckdb database/superbadd.duckdb
-- (VS Code DuckDB extension: use workspace database "superbadd" → database/superbadd.duckdb)

-- 0) Tables present in this file
SHOW TABLES;

-- 1) Row counts per table (one result set — catches empty loads or missing ingest)
SELECT 'gdw' AS tbl, COUNT(*)::BIGINT AS n FROM gdw
UNION ALL SELECT 'basinatlas', COUNT(*)::BIGINT FROM basinatlas
UNION ALL SELECT 'riveratlas', COUNT(*)::BIGINT FROM riveratlas
UNION ALL SELECT 'ffr', COUNT(*)::BIGINT FROM ffr
UNION ALL SELECT 'fhred', COUNT(*)::BIGINT FROM fhred
UNION ALL SELECT 'icold', COUNT(*)::BIGINT FROM icold
ORDER BY tbl;

-- 2) Join smoke test: dam and reach share hyriv_id, so river_name should fill in
SELECT g.dam_name, r.river_name
FROM gdw AS g
JOIN riveratlas AS r ON g.hyriv_id = r.hyriv_id
WHERE g.dam_name IS NOT NULL
LIMIT 5;
