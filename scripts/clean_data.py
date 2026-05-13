"""
Export cleaned layers from file geodatabases to GeoParquet.

We use Parquet (not CSV) so geometry stays in a standard binary form and DuckDB
can load it as real GEOMETRY types. Run from repo root: python scripts/clean_data.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "clean"


def clean_names(df):
    """Lowercase column names and replace spaces — matches SQL style and avoids case bugs in queries."""
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    # --- Global Dam Watch  ---
    gdw = clean_names(gpd.read_file(RAW / "GDW_v1_0.gdb", layer="GDW_barriers_v1_0"))
    # GDW uses -99 as a missing-value flag; turn those into real NaNs for analysis.
    gdw = gdw.replace([-99, "-99"], np.nan)
    gdw.to_parquet(OUT / "gdw.parquet")

    # --- HydroATLAS basins ---
    clean_names(gpd.read_file(RAW / "BasinATLAS_v10.gdb", layer="BasinATLAS_v10_lev12")).to_parquet(
        OUT / "basinatlas.parquet"
    )

    # --- HydroATLAS river reaches ---
    clean_names(gpd.read_file(RAW / "RiverATLAS_v10.gdb", layer="RiverATLAS_v10")).to_parquet(
        OUT / "riveratlas.parquet"
    )

    # --- FFR river network (rename so it joins to GDW on hyriv_id) ---
    ffr = clean_names(gpd.read_file(RAW / "FFR_river_network.gdb", layer="FFR_river_network_v1")).rename(
        columns={"reach_id": "hyriv_id"}
    )
    ffr.to_parquet(OUT / "ffr.parquet")

    # --- Future dams (FHrED): World Register of Dams spreadsheet in data/raw/ ---
    fhred_xlsx = RAW / "world_register_dams_2025.xlsx"

    if fhred_xlsx.is_file():
        fhred = clean_names(pd.read_excel(fhred_xlsx))
        if "dam_name" in fhred.columns:
            fhred["dam_name"] = fhred["dam_name"].astype(str)
        fhred.to_parquet(OUT / "fhred.parquet")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
