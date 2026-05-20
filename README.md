# Super Basin Atlas Dam Database (SuperBADD)

<img width="1354" height="710" alt="image" src="https://github.com/user-attachments/assets/6faa525d-1c4d-4498-b4c8-74236138aac0" />


The purpose of the **Super Basin Atlas Dam Database** links two major global current dam datasets (Global Dam Watch and ICOLD) to river-network context from HydroATLAS and connectivity metrics from the Free-Flowing Rivers dataset. These large datasets are can be very computationally expensive to work with, the goal of SuperBADD is to enable quick SQL-based exploration of how river attributes relate to the dams.

## Data Access

Raw data is free and accessible online at the links below. Obtain **GDW v1.0**, **HydroATLAS v10** (BasinATLAS + RiverATLAS), and **FFR v1** from Global Dam Watch / HydroSHEDS, as well as **FHReD** and **ICOLD** spreadsheets; place them under `data/raw/` using the names in `scripts/clean_data.py`. 

| Table | Source | Role |
|--------|--------|------|
| `gdw` | [Global Dam Watch](https://www.globaldamwatch.org/) | Dam points, discharge, capacity, `hyriv_id` |
| `riveratlas` | [HydroATLAS RiverATLAS](https://www.hydrosheds.org/hydroatlas) | Reach geometry, `main_riv`, `hybas_l12`, climate/terrain covariates |
| `basinatlas` | [HydroATLAS BasinATLAS](https://www.hydrosheds.org/hydroatlas) | Basin polygons, `hybas_id` |
| `ffr` | [Free-Flowing Rivers](https://figshare.com/articles/dataset/Mapping_the_world_s_free-flowing_rivers_data_set_and_technical_documentation/7688801) | Segment-scale connectivity and discharge metrics on `hyriv_id` |
| `fhred` | [FHReD 2015](https://www.globaldamwatch.org/fhred/) | Planned / future dams → `fhred.parquet` |
| `icold` | [ICOLD World Register](https://www.icold-cigb.org/GB/publications/world_register_of_dams.asp) | Alternative Current-dam register (separate from GDW) → `icold.parquet` |

## Logical schema and keys

DuckDB builds tables from Parquet without explicit `PRIMARY KEY` clauses; below is how joins are **meant** to work in analysis.

| Table | Natural key (concept) | Joins on |
|--------|------------------------|-----------|
| `gdw` | One row per dam (`gdw_id` is the dam id) | `hyriv_id` → `riveratlas.hyriv_id`, `ffr.hyriv_id` |
| `riveratlas` | One row per river reach | `hyriv_id`; `hybas_l12` → `basinatlas.hybas_id`; `main_riv` tags the whole river system |
| `basinatlas` | One row per basin polygon | `hybas_id` |
| `ffr` | One row per reach in the FFR network | `hyriv_id` (same id space as RiverATLAS / GDW) |
| `fhred` | One row per future-dam record (spreadsheet) |  **coordinate** key coming soon! |
| `icold` | One row per register record (spreadsheet) | **coordinate** key coming soon! |

### Entity-relationship diagram

```mermaid
flowchart LR
  gdw[gdw]
  riv[riveratlas]
  bas[basinatlas]
  ffr[ffr]
  fhred[fhred]
  icold[icold]
  gdw -->|"hyriv_id"| riv
  riv -->|"hybas_l12 = hybas_id"| bas
  gdw -->|"hyriv_id"| ffr
  riv -.->|"coords TBD"| ffr
  riv -.->|"coords TBD"| icold
```

## Repo Organization and docs are organized as follows:

- `scripts/clean_data.py` — raw inputs → `data/clean/*.parquet`
- `scripts/ingest.py` — Parquet → `database/superbadd.duckdb`
- `sql/` — analytical, verification, and join-template SQL
- `notebooks/data_cleaning.ipynb` — runs the cleaning script
- `notebooks/dam_river_network.ipynb` — maps + summaries + Ibis/SQL example
- `workflow/build_db.sql` — optional CLI mirror of ingest
- `requirements.txt` — Python packages (`pip install -r requirements.txt`)

## Reproduce

1. **Python 3.10+** and a virtual environment.
2. `pip install -r requirements.txt`.
3. Put source files under `data/raw/` (paths and sheet rules live in `scripts/clean_data.py`).
4. From the **repository root**:

```bash
python scripts/clean_data.py
python scripts/ingest.py
```

5. Open the notebooks with the repo root (`/SuperBasinDamDatabase`) as the working directory when possible.



## References

- This repository was made in the course **EDS 213: Databases and Data Management** at the [UCSB Bren School](https://bren.ucsb.edu/) as part of the Masters of Environmental Data Science Program.
- Huge thank you to the creators of the source datasets: **Global Dam Watch**, **HydroATLAS**, **Free-Flowing Rivers**, **FHReD**, and **ICOLD**.
- And the professors and TAs who helped with the workflow, SQL, and Python: **Annie Adams**, **Julien Brun** and **Greg Janee**.

