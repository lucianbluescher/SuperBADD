-- Optional: duckdb database/superbadd.duckdb < workflow/build_db.sql
-- Prefer: python scripts/ingest.py

INSTALL spatial;
LOAD spatial;

CREATE OR REPLACE TABLE gdw AS SELECT * FROM read_parquet('data/clean/gdw.parquet');
CREATE OR REPLACE TABLE basinatlas AS SELECT * FROM read_parquet('data/clean/basinatlas.parquet');
CREATE OR REPLACE TABLE riveratlas AS SELECT * FROM read_parquet('data/clean/riveratlas.parquet');
CREATE OR REPLACE TABLE ffr AS SELECT * FROM read_parquet('data/clean/ffr.parquet');
CREATE OR REPLACE TABLE fhred AS SELECT * FROM read_parquet('data/clean/fhred.parquet');
CREATE OR REPLACE TABLE icold AS SELECT * FROM read_parquet('data/clean/icold.parquet');
