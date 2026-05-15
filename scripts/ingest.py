"""Load every `data/clean/<name>.parquet` into `database/superbadd.duckdb` (see `TABLES`)."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data" / "clean"
DB_PATH = ROOT / "database" / "superbadd.duckdb"

TABLES = ("gdw", "basinatlas", "riveratlas", "ffr", "fhred", "icold")

# Tables with no geometry: skip spatial extension when the ingest run is these.
TABULAR_TABLES = frozenset({"fhred", "icold"})


def connect_db():
    try:
        return duckdb.connect(str(DB_PATH))
    except Exception as e:
        err = str(e).lower()
        if "lock" in err or "conflicting" in err:
            print(
                "\nThis database file is already open elsewhere\n",
                file=sys.stderr,
            )
        raise


def load_spatial(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL spatial; LOAD spatial;")


def _is_broken_geoparquet_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return "geoparquet" in msg or "does not have geometry types" in msg


def ingest_from_wkb_blob(con: duckdb.DuckDBPyConnection, name: str, pq: Path) -> None:
    """Load Parquet whose geometry column is WKB BLOB without GeoParquet metadata."""
    stage = duckdb.connect()
    try:
        stage.execute("CREATE TABLE _stage AS SELECT * FROM read_parquet(?)", [str(pq)])
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            temp_pq = Path(tmp.name)
        stage.execute(f"COPY _stage TO '{temp_pq.as_posix()}' (FORMAT PARQUET)")
    finally:
        stage.close()

    try:
        con.execute(f"DROP TABLE IF EXISTS {name};")
        con.execute(
            f"""
            CREATE TABLE {name} AS
            SELECT * EXCLUDE (geometry), ST_GeomFromWKB(geometry) AS geometry
            FROM read_parquet(?)
            """,
            [str(temp_pq)],
        )
    finally:
        if temp_pq.is_file():
            temp_pq.unlink()


def ingest_table(con: duckdb.DuckDBPyConnection, name: str, pq: Path) -> None:
    con.execute(f"DROP TABLE IF EXISTS {name};")
    if name in TABULAR_TABLES:
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet(?);", [str(pq)])
        return

    load_spatial(con)
    try:
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet(?);", [str(pq)])
    except duckdb.Error as exc:
        if not _is_broken_geoparquet_error(exc):
            raise
        print(
            f"note {name}: GeoParquet metadata invalid; loading geometry via WKB (re-run clean_data.py to fix the file)",
            file=sys.stderr,
        )
        ingest_from_wkb_blob(con, name, pq)


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
        created: list[str] = []
        skipped: list[str] = []
        for name in tables:
            pq = CLEAN / f"{name}.parquet"
            if not pq.is_file():
                skipped.append(name)
                print(f"skip {name}: missing {pq}", file=sys.stderr)
                print("  Run `python scripts/clean_data.py` so Parquet files exist under data/clean/.", file=sys.stderr)
                if len(tables) == 1:
                    sys.exit(1)
                continue
            ingest_table(con, name, pq)
            created.append(name)

        for name in tables:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                print(f"{name}\t{n}")
            except duckdb.CatalogException:
                pass

        if skipped:
            print(
                f"Ingest summary: created {len(created)} table(s) {created}; skipped {len(skipped)} {skipped}. "
                "SHOW TABLES in DuckDB will only list created names.",
                file=sys.stderr,
            )
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
