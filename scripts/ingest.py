"""Load every `data/clean/<name>.parquet` into `database/superbadd.duckdb` (see `TABLES`)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data" / "clean"
DB_PATH = ROOT / "database" / "superbadd.duckdb"

TABLES = ("gdw", "basinatlas", "riveratlas", "ffr", "fhred", "icold")

# Tables with no geometry: skip spatial extension when the ingest run is only these (faster).
TABULAR_TABLES = frozenset({"fhred", "icold"})


def connect_db():
    try:
        return duckdb.connect(str(DB_PATH))
    except Exception as e:
        err = str(e).lower()
        if "lock" in err or "conflicting" in err:
            print(
                "\nThis database file is already open elsewhere. DuckDB allows one writer at a time.\n"
                "Close the other app, then retry:\n"
                "  • Jupyter — shut down the kernel using this project (or restart Jupyter).\n"
                "  • Cursor / VS Code — close the DuckDB panel / SQL session attached to superbadd.duckdb.\n"
                "  • Another terminal — exit Python or stop the process shown in the error above.\n",
                file=sys.stderr,
            )
        raise


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    p = argparse.ArgumentParser(description="Load GeoParquet into DuckDB.")
    p.add_argument(
        "--only",
        metavar="TABLE",
        help="Comma-separated table names; default is all tables in TABLES.",
    )
    args = p.parse_args(argv)

    if args.only:
        tables = [t.strip() for t in args.only.split(",") if t.strip()]
        bad = [t for t in tables if t not in TABLES]
        if bad:
            print(f"Unknown table(s): {bad}. Allowed: {list(TABLES)}", file=sys.stderr)
            sys.exit(1)
    else:
        tables = list(TABLES)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = None
    try:
        con = connect_db()
        if not set(tables) <= TABULAR_TABLES:
            con.execute("INSTALL spatial; LOAD spatial;")

        for name in tables:
            pq = CLEAN / f"{name}.parquet"
            if not pq.is_file():
                print(f"skip {name}: missing {pq}", file=sys.stderr)
                print("  Run `python scripts/clean_data.py` so Parquet files exist under data/clean/.", file=sys.stderr)
                if len(tables) == 1:
                    sys.exit(1)
                continue
            con.execute(f"DROP TABLE IF EXISTS {name};")
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet(?);", [str(pq)])

        for name in tables:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                print(f"{name}\t{n}")
            except duckdb.CatalogException:
                pass
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
