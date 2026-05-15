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
    """Lowercase column names and replace spaces."""
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df


def write_geoparquet(gdf: gpd.GeoDataFrame, path: Path) -> None:
    """Write GeoParquet so DuckDB spatial can read geometry (not plain WKB BLOB)."""
    if gdf.geometry.name != "geometry":
        gdf = gdf.rename_geometry("geometry")
    gdf.to_parquet(path, index=False)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    # --- Global Dam Watch  ---
    gdw = clean_names(gpd.read_file(RAW / "GDW_v1_0.gdb", layer="GDW_barriers_v1_0"))
    # GDW uses -99 as a missing-value flag; turn those into real NaNs for analysis.
    gdw = gdw.replace([-99, "-99"], np.nan)
    write_geoparquet(gdw, OUT / "gdw.parquet")

    # --- HydroATLAS basins ---
    write_geoparquet(
        clean_names(gpd.read_file(RAW / "BasinATLAS_v10.gdb", layer="BasinATLAS_v10_lev12")),
        OUT / "basinatlas.parquet",
    )

    # --- HydroATLAS river reaches ---
    write_geoparquet(
        clean_names(gpd.read_file(RAW / "RiverATLAS_v10.gdb", layer="RiverATLAS_v10")),
        OUT / "riveratlas.parquet",
    )

    # --- FFR river network (rename so it joins to GDW on hyriv_id) ---
    ffr = clean_names(gpd.read_file(RAW / "FFR_river_network.gdb", layer="FFR_river_network_v1")).rename(
        columns={"reach_id": "hyriv_id"}
    )
    write_geoparquet(ffr, OUT / "ffr.parquet")

    # --- ICOLD World Register (current dams) — README: world_register_dams_2025.xlsx → icold.parquet
    wr_xlsx = RAW / "world_register_dams_2025.xlsx"
    if wr_xlsx.is_file():
        icold = clean_names(pd.read_excel(wr_xlsx, sheet_name=0))
        if "dam_name" in icold.columns:
            icold["dam_name"] = icold["dam_name"].astype(str)
        for col in icold.select_dtypes(include=["object"]).columns:
            icold[col] = icold[col].astype(str)
        icold.to_parquet(OUT / "icold.parquet")

    # --- FHReD future dams — README: FHReD_2015_future_dams.xlsx, 2nd worksheet → fhred.parquet
    fhred_candidates = sorted(RAW.glob("FHReD_2015_future_dams.xlsx")) + sorted(
        RAW.glob("fhred_2015_future_dams.xlsx")
    )
    if fhred_candidates:
        fhred = clean_names(pd.read_excel(fhred_candidates[0], sheet_name=1))
        if "dam_name" in fhred.columns:
            fhred["dam_name"] = fhred["dam_name"].astype(str)
        for col in fhred.select_dtypes(include=["object"]).columns:
            fhred[col] = fhred[col].astype(str)
        fhred.to_parquet(OUT / "fhred.parquet")
    elif wr_xlsx.is_file():
        xl = pd.ExcelFile(wr_xlsx)
        if len(xl.sheet_names) > 1:
            fhred = clean_names(pd.read_excel(wr_xlsx, sheet_name=1))
            if "dam_name" in fhred.columns:
                fhred["dam_name"] = fhred["dam_name"].astype(str)
            for col in fhred.select_dtypes(include=["object"]).columns:
                fhred[col] = fhred[col].astype(str)
            fhred.to_parquet(OUT / "fhred.parquet")
        else:
            print(
                "skip fhred: add data/raw/FHReD_2015_future_dams.xlsx (2nd sheet) "
                "or a 2nd sheet on world_register_dams_2025.xlsx",
                file=sys.stderr,
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
