"""
DuckDB analytical queries over the processed tree canopy mart.

Loads data/processed/mart_canopy_coverage.parquet directly into DuckDB
(zero-copy via Arrow), runs three analytical queries, writes combined
results to data/output/duckdb_results.csv, and mocks an S3 upload by
copying the Parquet file into data/mock_s3/.

Run:
    python queries/duckdb_analysis.py
"""

import logging
import os
import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
PARQUET_IN = BASE_DIR / "data" / "processed" / "mart_canopy_coverage.parquet"
OUTPUT_DIR = BASE_DIR / "data" / "output"
MOCK_S3_DIR = BASE_DIR / "data" / "mock_s3"
CSV_OUT = OUTPUT_DIR / "duckdb_results.csv"

_pg_host = os.getenv("POSTGIS_HOST", "localhost")
DB_URL = os.getenv(
    "TREECANOPY_DB_URL",
    f"postgresql+psycopg2://danish:danish123@{_pg_host}:5432/treecanopy",
)

# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------
QUERIES: dict[str, str] = {
    "top10_canopy_density": """
        SELECT
            census_tract_id,
            boroname,
            tree_count,
            species_count,
            round(canopy_density_score, 4) AS canopy_density_score,
            round(avg_ndvi, 4)             AS avg_ndvi
        FROM canopy
        ORDER BY canopy_density_score DESC
        LIMIT 10
    """,
    "avg_ndvi_by_borough": """
        SELECT
            boroname,
            round(avg(avg_ndvi), 4)   AS avg_ndvi,
            sum(tree_count)           AS total_trees,
            sum(species_count)        AS total_species
        FROM canopy
        GROUP BY boroname
        ORDER BY avg_ndvi DESC
    """,
    "species_vs_tree_count": """
        SELECT
            boroname,
            species_count,
            tree_count
        FROM canopy
        ORDER BY tree_count DESC
    """,
}


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------
def refresh_mart_parquet(dest: Path, db_url: str) -> bool:
    """Export analytics.mart_canopy_coverage from PostGIS to Parquet.

    Mirrors what the Airflow export_parquet task does. Called at startup so
    local runs always read fresh data without needing Airflow running.

    Returns True if the export succeeded, False if PostGIS was unreachable.

    Args:
        dest: Destination Parquet path.
        db_url: SQLAlchemy connection string for the PostGIS database.
    """
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM analytics.mart_canopy_coverage"))
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), dest)
        log.info("Refreshed mart Parquet from PostGIS → %s (%d rows)", dest, len(df))
        return True
    except Exception as exc:
        log.warning("PostGIS refresh skipped (%s) — reading existing Parquet", exc)
        return False


def load_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Register the Parquet file as a DuckDB table named 'canopy'.

    Args:
        con: Open DuckDB in-memory connection.
        path: Path to mart_canopy_coverage.parquet.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet not found: {path}\n"
            "Run the full pipeline first: python ingestion/process_and_load.py"
        )
    log.info("Loading parquet from %s", path)
    con.execute(f"CREATE TABLE canopy AS SELECT * FROM read_parquet('{path}')")
    count = con.execute("SELECT count(*) FROM canopy").fetchone()[0]
    log.info("Loaded %d rows into DuckDB table 'canopy'", count)


def run_query(
    con: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame.

    Args:
        con: Open DuckDB connection with 'canopy' table registered.
        name: Human-readable query name (used as query_name column).
        sql: SQL string to execute.

    Returns:
        DataFrame of results with an added 'query_name' column.
    """
    log.info("Running query: %s", name)
    df = con.execute(sql).df()
    df.insert(0, "query_name", name)
    log.info("  → %d rows returned", len(df))
    return df


def print_results(name: str, df: pd.DataFrame) -> None:
    """Pretty-print a query result to stdout.

    Args:
        name: Query label for the header.
        df: DataFrame to display.
    """
    display = df.drop(columns=["query_name"], errors="ignore")
    print(f"\n{'='*60}")
    print(f"  {name.upper().replace('_', ' ')}")
    print(f"{'='*60}")
    print(display.to_string(index=False))


def save_results(frames: list[pd.DataFrame], dest: Path) -> None:
    """Concatenate all result DataFrames and write to a single CSV.

    Args:
        frames: List of DataFrames, each with a 'query_name' column.
        dest: Output CSV path.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv(dest, index=False)
    log.info("Saved %d total rows to %s", len(combined), dest)


def export_to_parquet_s3_mock(src: Path, mock_s3_dir: Path) -> None:
    """Simulate an S3 upload by copying the Parquet file to data/mock_s3/.

    In production this would use boto3.client('s3').upload_file().
    The mock keeps the same bucket/key structure for easy substitution.

    Args:
        src: Source Parquet file to 'upload'.
        mock_s3_dir: Local directory acting as the S3 bucket root.
    """
    bucket = "tree-canopy-bucket"
    key = "marts/mart_canopy_coverage.parquet"
    dest = mock_s3_dir / key
    dest.parent.mkdir(parents=True, exist_ok=True)

    log.info("Mock S3 upload: %s → s3://%s/%s", src.name, bucket, key)
    shutil.copy2(src, dest)
    print(f"\nUploaded to s3://{bucket}/{key} (mocked locally at {dest})")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    """Run all DuckDB analyses and export results."""
    refresh_mart_parquet(PARQUET_IN, DB_URL)

    con = duckdb.connect()  # in-memory

    try:
        load_parquet(con, PARQUET_IN)

        frames: list[pd.DataFrame] = []
        for name, sql in QUERIES.items():
            df = run_query(con, name, sql)
            print_results(name, df)
            frames.append(df)

        save_results(frames, CSV_OUT)
        export_to_parquet_s3_mock(PARQUET_IN, MOCK_S3_DIR)

    finally:
        con.close()

    print(f"\n--- DuckDB analysis complete ---")
    print(f"  CSV results : {CSV_OUT.resolve()}")
    print(f"  Mock S3     : {(MOCK_S3_DIR / 'marts' / 'mart_canopy_coverage.parquet').resolve()}")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as exc:
        log.error("%s", exc)
        sys.exit(1)
    except Exception as exc:
        log.error("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)
