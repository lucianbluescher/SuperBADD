
import geopandas as gpd
import pandas as pd
import numpy as np

def clean_names(df):
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df
# --- Global Dam Watch ---
gdw_path = "data/raw/GDW_v1_0.gdb"
gdw = clean_names(gpd.read_file(gdw_path, layer="GDW_barriers_v1_0"))

# Replace -99 and "-99" with NaN
gdw = gdw.replace([-99, "-99"], np.nan)

# Save as Parquet
gdw.to_parquet('data/clean/gdw.parquet')

# --- BasinATLAS ---
basinatlas_path = "data/raw/BasinATLAS_v10.gdb"
basinatlas = clean_names(gpd.read_file(basinatlas_path, layer="BasinATLAS_v10_lev12"))

# Save as Parquet
basinatlas.to_parquet('data/clean/basinatlas.parquet')
# --- RiverATLAS ---
riveratlas_path = "data/raw/RiverATLAS_v10.gdb"
riveratlas = clean_names(gpd.read_file(riveratlas_path, layer="RiverATLAS_v10"))

# Save as Parquet
riveratlas.to_parquet('data/clean/riveratlas.parquet')
# --- Free-Flowing Rivers ---
ffr_path = "data/raw/FFR_river_network.gdb"
ffr = clean_names(gpd.read_file(ffr_path, layer="FFR_river_network_v1")).rename(columns={'reach_id': 'hyriv_id'})
