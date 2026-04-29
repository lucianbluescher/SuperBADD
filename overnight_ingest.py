import geopandas as gpd
import pandas as pd
import numpy as np
import duckdb
import os
import time

# Create directories if they don't exist
os.makedirs('data/clean', exist_ok=True)

def clean_names(df):
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# =============================================================================
# STAGE 1: CLEANING & PARQUET EXPORT
# =============================================================================

datasets = [
    {"name": "GDW", "path": "data/raw/GDW_v1_0.gdb", "layer": "GDW_barriers_v1_0", "type": "gdb"},
    {"name": "BasinATLAS", "path": "data/raw/BasinATLAS_v10.gdb", "layer": "BasinATLAS_v10_lev12", "type": "gdb"},
    {"name": "RiverATLAS", "path": "data/raw/RiverATLAS_v10.gdb", "layer": "RiverATLAS_v10", "type": "gdb"},
    {"name": "FFR", "path": "data/raw/FFR_river_network.gdb", "layer": "FFR_river_network_v1", "type": "gdb"},
    {"name": "FHrED", "path": "data/raw/world_register_dams2025.xlsx", "type": "xlsx"}
]

for ds in datasets:
    log(f"Processing {ds['name']}...")
    try:
        if ds['type'] == 'gdb':
            df = gpd.read_file(ds['path'], layer=ds['layer'])
        else:
            df = pd.read_excel(ds['path'])
        
        df = clean_names(df)
        
        if ds['name'] == 'GDW':
            df = df.replace([-99, "-99"], np.nan)
        if ds['name'] == 'FFR':
            df = df.rename(columns={'reach_id': 'hyriv_id'})
            
        out_path = f"data/clean/{ds['name'].lower()}.parquet"
        df.to_parquet(out_path)
        log(f"Finished {ds['name']}. Saved to {out_path}")
        
        # Free up memory immediately
        del df
    except Exception as e:
        log(f"ERROR processing {ds['name']}: {e}")

# =============================================================================
# STAGE 2: DUCKDB INGESTION
# =============================================================================

log("Starting DuckDB Ingestion...")
con = duckdb.connect('superbadd.db')

# Setup Spatial
con.execute("INSTALL spatial; LOAD spatial;")

tables = ['gdw', 'basinatlas', 'riveratlas', 'ffr', 'fhred']

for table in tables:
    log(f"Ingesting {table} into database...")
    con.execute(f"DROP TABLE IF EXISTS {table};")
    con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('data/clean/{table}.parquet');")

# =============================================================================
# STAGE 3: VERIFICATION
# =============================================================================

log("Running verifications...")
verify_query = """
SELECT 'gdw' AS tbl, COUNT(*) AS rows FROM gdw UNION ALL
SELECT 'basinatlas', COUNT(*) FROM basinatlas UNION ALL
SELECT 'riveratlas', COUNT(*) FROM riveratlas UNION ALL
SELECT 'ffr', COUNT(*) FROM ffr UNION ALL
SELECT 'fhred', COUNT(*) FROM fhred;
"""
print(con.execute(verify_query).df())

log("Master Ingestion Complete. Database 'superbadd.db' is ready.")
con.close()