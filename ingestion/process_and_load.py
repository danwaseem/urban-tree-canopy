"""
End-to-end geospatial processing pipeline for the NYC 2015 Street Tree Census.

CRS order (important):
  1. Build tree points in EPSG:4326  (matches the raw CSV lat/lon).
  2. Sample NDVI raster in EPSG:4326 (raster is stored in 4326).
  3. Reproject trees to EPSG:32618   (UTM zone 18N, metre-based, good for NYC).
  4. Reproject census tracts to EPSG:32618 before spatial join.
"""

import io
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import rasterio.sample
from shapely.geometry import Point, box
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths & connection
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
RAW_CSV = BASE_DIR / "data" / "raw" / "nyc_tree_census_2015.csv"
NDVI_TIF = BASE_DIR / "data" / "raw" / "nyc_ndvi_synthetic.tif"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PARQUET_OUT = PROCESSED_DIR / "tree_census_processed.parquet"

# POSTGIS_HOST is 'localhost' for local dev and 'postgis' inside Docker
# (set via docker-compose.yml environment). Mirrors the same env_var used
# in dbt profiles.yml so both tools resolve the host identically.
_pg_host = os.getenv("POSTGIS_HOST", "localhost")
DB_URL = f"postgresql+psycopg2://danish:danish123@{_pg_host}:5432/treecanopy"

# Columns to keep from the CSV.
# The raw CSV has 'borough' (not 'boroname'); borocode kept as numeric fallback.
# created_at = survey timestamp; tree_dbh = diameter at breast height (canopy proxy).
KEEP_COLS = [
    "tree_id", "block_id", "created_at", "status", "health",
    "spc_common", "spc_latin", "tree_dbh",
    "latitude", "longitude", "address", "postcode",
    "borough", "borocode", "nta", "nta_name", "census_tract",
]

BOROCODE_MAP: dict[int, str] = {
    1: "Manhattan",
    2: "Bronx",
    3: "Brooklyn",
    4: "Queens",
    5: "Staten Island",
}

# Columns that dbt models require — must always be present in raw.tree_census
REQUIRED_DOWNSTREAM_COLS = [
    "tree_id", "status", "health", "spc_common",
    "latitude", "longitude", "boroname", "census_tract_id", "ndvi_value",
]


# ---------------------------------------------------------------------------
# Step 1 — Load CSV
# ---------------------------------------------------------------------------
def load_tree_csv(path: Path, max_rows: Optional[int] = 100_000) -> pd.DataFrame:
    """Load the tree census CSV, standardise column names, and drop incomplete rows.

    Args:
        path: Path to nyc_tree_census_2015.csv.
        max_rows: Cap rows for local development. Pass None to load everything.

    Returns:
        Cleaned DataFrame with at least latitude and longitude columns.
    """
    log.info("Loading tree census CSV from %s", path)
    df = pd.read_csv(path, low_memory=False, nrows=max_rows)

    # Normalise column names: lowercase + underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    log.info("Raw shape: %d rows x %d cols", len(df), len(df.columns))

    # Keep only columns that actually exist in this dataset
    present = [c for c in KEEP_COLS if c in df.columns]
    missing = set(KEEP_COLS) - set(present)
    if missing:
        log.warning("Columns not found in CSV (will be skipped): %s", missing)
    df = df[present].copy()

    before = len(df)
    df = df.dropna(subset=["latitude", "longitude"])
    dropped = before - len(df)
    if dropped:
        log.info("Dropped %d rows with missing lat/lon", dropped)

    log.info("Loaded %d trees", len(df))
    return df


# ---------------------------------------------------------------------------
# Step 1b — Guarantee required downstream columns exist
# ---------------------------------------------------------------------------
def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all columns required by dbt models are present in the DataFrame.

    Derives boroname using a priority chain so values are never all UNKNOWN:
      1. borough column  (present in the NYC 2015 CSV: Queens, Brooklyn, etc.)
      2. borocode column (int 1-5 mapped via BOROCODE_MAP)
      3. 'UNKNOWN'       (last resort)

    Args:
        df: Cleaned tree DataFrame from load_tree_csv().

    Returns:
        DataFrame with all required columns guaranteed to exist.
    """
    df = df.copy()

    # Raw CSV uses 'borough'; derive boroname from it directly.
    if "borough" in df.columns and df["borough"].notna().any():
        log.info("Deriving boroname from 'borough' column")
        df["boroname"] = df["borough"]
    elif "borocode" in df.columns and df["borocode"].notna().any():
        log.info("Deriving boroname from 'borocode' via BOROCODE_MAP")
        df["boroname"] = df["borocode"].map(BOROCODE_MAP).fillna("UNKNOWN")
    else:
        log.warning("No borough source found — setting boroname = 'UNKNOWN'")
        df["boroname"] = "UNKNOWN"

    log.info("boroname value counts:\n%s", df["boroname"].value_counts().to_string())

    # Log which required cols are still absent (census_tract_id / ndvi_value
    # are added later by the pipeline so skip them here)
    pipeline_added = {"census_tract_id", "ndvi_value"}
    still_missing = [
        c for c in REQUIRED_DOWNSTREAM_COLS
        if c not in df.columns and c not in pipeline_added
    ]
    if still_missing:
        log.warning("Required columns absent after CSV load: %s", still_missing)

    return df


# ---------------------------------------------------------------------------
# Step 2 — Build GeoDataFrame in EPSG:4326
# ---------------------------------------------------------------------------
def build_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert tree DataFrame to a GeoDataFrame with Point geometries in EPSG:4326.

    Args:
        df: Cleaned tree DataFrame with latitude and longitude columns.

    Returns:
        GeoDataFrame in EPSG:4326.
    """
    log.info("Building GeoDataFrame (EPSG:4326)")
    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    log.info("GeoDataFrame CRS: %s", gdf.crs)
    return gdf


# ---------------------------------------------------------------------------
# Step 3 — Sample NDVI (while still in EPSG:4326)
# ---------------------------------------------------------------------------
def sample_ndvi(gdf: gpd.GeoDataFrame, tif_path: Path) -> gpd.GeoDataFrame:
    """Sample synthetic NDVI values at each tree location.

    The raster is in EPSG:4326, so coordinates must NOT be reprojected
    before sampling. Call this function before any to_crs() call.

    Args:
        gdf: Tree GeoDataFrame in EPSG:4326.
        tif_path: Path to the NDVI GeoTIFF (EPSG:4326).

    Returns:
        GeoDataFrame with an added 'ndvi_value' column (float32).
    """
    assert str(gdf.crs.to_epsg()) == "4326", "Sample NDVI before reprojecting to 32618!"

    log.info("Sampling NDVI from %s", tif_path)
    with rasterio.open(tif_path) as ds:
        raster_epsg = ds.crs.to_epsg()
        log.info("Raster CRS: EPSG:%s", raster_epsg)
        if raster_epsg != 4326:
            raise ValueError(f"Expected raster EPSG:4326, got EPSG:{raster_epsg}")

        coords = [(geom.x, geom.y) for geom in gdf.geometry]
        sampled = list(ds.sample(coords, indexes=1))

    ndvi_values = np.array([v[0] for v in sampled], dtype=np.float32)

    # Mark out-of-bounds or nodata as NaN
    nodata = -9999.0
    ndvi_values = np.where(ndvi_values == nodata, np.nan, ndvi_values)

    gdf = gdf.copy()
    gdf["ndvi_value"] = ndvi_values

    valid = np.sum(~np.isnan(ndvi_values))
    log.info("NDVI sampled: %d/%d valid values", valid, len(gdf))

    # Validate range
    valid_vals = ndvi_values[~np.isnan(ndvi_values)]
    if len(valid_vals) > 0:
        out_of_range = np.sum((valid_vals < -1) | (valid_vals > 1))
        if out_of_range:
            log.warning("%d NDVI values outside [-1, 1]", out_of_range)

    return gdf


# ---------------------------------------------------------------------------
# Step 4 — Census tract fallback polygons
# ---------------------------------------------------------------------------
def load_census_tracts() -> gpd.GeoDataFrame:
    """Return a GeoDataFrame of synthetic census-tract-like grid cells covering NYC.

    NOTE: This is a synthetic fallback for local/demo purposes only.
    In production, replace this with real Census TIGER tract boundaries from:
      https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

    The grid approximates the NYC bounding box at ~0.05 degree resolution,
    producing ~130 cells that cover all five boroughs. Each cell gets a unique
    census_tract_id. Polygons are created in EPSG:4326.

    Returns:
        GeoDataFrame of tract polygons in EPSG:4326 with column 'census_tract_id'.
    """
    log.info("Building synthetic NYC census tract grid (fallback — not real TIGER data)")

    lon_min, lon_max = -74.26, -73.70
    lat_min, lat_max = 40.49, 40.92
    step = 0.05  # ~4 km per cell at NYC latitude

    lons = np.arange(lon_min, lon_max, step)
    lats = np.arange(lat_min, lat_max, step)

    polygons = []
    tract_ids = []
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            polygons.append(box(lon, lat, lon + step, lat + step))
            tract_ids.append(f"TRACT_{i:02d}_{j:02d}")

    gdf = gpd.GeoDataFrame(
        {"census_tract_id": tract_ids},
        geometry=polygons,
        crs="EPSG:4326",
    )
    log.info("Created %d synthetic census tract polygons", len(gdf))
    return gdf


# ---------------------------------------------------------------------------
# Step 5 — Reproject trees to EPSG:32618
# ---------------------------------------------------------------------------
def reproject_trees(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject tree GeoDataFrame from EPSG:4326 to EPSG:32618 (UTM Zone 18N).

    Call this AFTER NDVI sampling, which requires EPSG:4326.

    Args:
        gdf: Tree GeoDataFrame currently in EPSG:4326.

    Returns:
        GeoDataFrame reprojected to EPSG:32618.
    """
    log.info("Reprojecting trees to EPSG:32618 (UTM Zone 18N)")
    gdf_proj = gdf.to_crs(epsg=32618)
    assert gdf_proj.crs.to_epsg() == 32618, "Reprojection failed"
    log.info("Tree CRS after reprojection: %s", gdf_proj.crs)
    return gdf_proj


# ---------------------------------------------------------------------------
# Step 6 — Spatial join
# ---------------------------------------------------------------------------
def spatial_join(
    trees: gpd.GeoDataFrame,
    tracts: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Assign census_tract_id to each tree via a spatial join.

    Both GeoDataFrames must be in EPSG:32618 before calling this function.
    Trees that fall outside all tract polygons get census_tract_id = 'UNKNOWN'.

    Args:
        trees: Tree GeoDataFrame in EPSG:32618.
        tracts: Census tract GeoDataFrame, will be reprojected to EPSG:32618 here.

    Returns:
        Tree GeoDataFrame with census_tract_id column added.
    """
    log.info("Reprojecting census tracts to EPSG:32618")
    tracts_proj = tracts.to_crs(epsg=32618)

    log.info("Running spatial join (predicate='within')")
    joined = gpd.sjoin(
        trees,
        tracts_proj[["census_tract_id", "geometry"]],
        how="left",
        predicate="within",
    )

    # Drop the right-side index column added by sjoin
    joined = joined.drop(columns=["index_right"], errors="ignore")

    unmatched = joined["census_tract_id"].isna().sum()
    if unmatched:
        log.warning("%d trees did not join to any tract — labelling as UNKNOWN", unmatched)
        joined["census_tract_id"] = joined["census_tract_id"].fillna("UNKNOWN")

    log.info(
        "Spatial join complete: %d trees, %d unique tracts",
        len(joined),
        joined["census_tract_id"].nunique(),
    )
    return joined


# ---------------------------------------------------------------------------
# Step 7 — Validate
# ---------------------------------------------------------------------------
def validate(gdf: gpd.GeoDataFrame) -> None:
    """Run assertions on the processed GeoDataFrame and log a summary.

    Args:
        gdf: Processed tree GeoDataFrame in EPSG:32618.

    Raises:
        AssertionError: If any data-quality check fails.
    """
    log.info("Running validation checks")

    assert gdf["tree_id"].notna().all(), "Null tree_id values found"
    assert gdf["latitude"].notna().all(), "Null latitude values found"
    assert gdf["longitude"].notna().all(), "Null longitude values found"
    assert gdf.geometry.is_valid.all(), "Invalid geometries found"
    assert gdf.crs.to_epsg() == 32618, f"Expected EPSG:32618, got {gdf.crs}"

    if "ndvi_value" in gdf.columns:
        valid_ndvi = gdf["ndvi_value"].dropna()
        bad = valid_ndvi[(valid_ndvi < -1) | (valid_ndvi > 1)]
        assert len(bad) == 0, f"{len(bad)} NDVI values outside [-1, 1]"

    log.info("All validation checks passed")

    print("\n--- process_and_load: validation summary ---")
    print(f"  Total trees          : {len(gdf):,}")
    print(f"  Trees with NDVI      : {gdf['ndvi_value'].notna().sum():,}")
    print(f"  Unique census tracts : {gdf['census_tract_id'].nunique()}")
    print(f"  CRS                  : {gdf.crs}")


# ---------------------------------------------------------------------------
# Step 8 — Write outputs
# ---------------------------------------------------------------------------
def write_postgis(gdf: gpd.GeoDataFrame, db_url: str) -> None:
    """Write the processed GeoDataFrame to PostGIS (raw.tree_census).

    Writes as a plain DataFrame (no PostGIS geometry column) so the write
    works identically across Python versions and GeoAlchemy2 versions.
    Spatial data is preserved in latitude, longitude, x_32618, y_32618.

    Creates the 'raw' and 'analytics' schemas if they don't exist.

    Args:
        gdf: Processed GeoDataFrame in EPSG:32618.
        db_url: SQLAlchemy connection string.
    """
    log.info("Connecting to PostGIS at %s", db_url.split("@")[-1])
    engine = create_engine(db_url)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        log.info("Ensured schemas: raw, analytics")
        # CASCADE drops any dependent views (e.g. analytics.stg_tree_census)
        # so the replace below is never blocked by dependent objects.
        conn.execute(text("DROP TABLE IF EXISTS raw.tree_census CASCADE"))
        log.info("Dropped raw.tree_census (CASCADE) — dependent views will be recreated by dbt")

    # Convert to plain DataFrame — drop geometry column to avoid GeoAlchemy2
    # version sensitivity (geometry type handling changed in 0.14+). Spatial
    # data is fully represented by latitude/longitude and x_32618/y_32618.
    df = pd.DataFrame(gdf.drop(columns=["geometry"]))
    df["x_32618"] = gdf.geometry.x
    df["y_32618"] = gdf.geometry.y

    # Add ingestion timestamp so dbt source freshness can use loaded_at_field
    df["loaded_at"] = pd.Timestamp.now("UTC")

    log.info("Writing %d rows to PostGIS table raw.tree_census", len(df))
    # Use psycopg2 copy_expert instead of df.to_sql — pandas in Python 3.12
    # (Airflow container) does not recognise SQLAlchemy 2.x Engine/Connection
    # objects and falls through to the DBAPI2 path calling .cursor(), which
    # raises AttributeError. copy_expert bypasses pandas entirely.
    cols = ", ".join(f'"{c}"' for c in df.columns)
    col_defs = ",\n    ".join(
        f'"{c}" TEXT' for c in df.columns
    )
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.tree_census (
                    {col_defs}
                )
            """)
            buf = io.StringIO()
            # na_rep='' (default) writes NaN as empty string; NULL '' tells
            # PostgreSQL to treat empty unquoted CSV fields as SQL NULL so that
            # downstream dbt casts (e.g. cast(tree_id as integer)) don't fail.
            df.to_csv(buf, index=False, header=False, na_rep="")
            buf.seek(0)
            cur.copy_expert(
                f"COPY raw.tree_census ({cols}) FROM STDIN WITH CSV NULL ''",
                buf,
            )
        raw_conn.commit()
    finally:
        raw_conn.close()
    log.info("PostGIS write complete")


def write_parquet(gdf: gpd.GeoDataFrame, dest: Path) -> None:
    """Serialise the processed GeoDataFrame to Parquet with WKB geometry.

    Geometry is stored as WKB bytes (well-known binary) so the Parquet file
    is self-contained and readable by GeoPandas, DuckDB, or any Arrow reader.
    x_32618 and y_32618 metre coordinates are added for convenience.

    Args:
        gdf: Processed GeoDataFrame in EPSG:32618.
        dest: Output path for the Parquet file.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    df = gdf.copy()

    # Store coordinates as plain floats for easy filtering without geometry libs
    df["x_32618"] = df.geometry.x
    df["y_32618"] = df.geometry.y

    # Serialise geometry to WKB bytes — readable by gpd.read_parquet / DuckDB
    df["geometry_wkb"] = df.geometry.apply(lambda g: g.wkb)
    df = df.drop(columns=["geometry"])

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, dest, compression="snappy")
    log.info("Parquet written to %s (%d rows)", dest, len(df))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def main(max_rows: Optional[int] = 100_000, require_postgis: bool = False) -> None:
    """Run the full processing pipeline end-to-end.

    Args:
        max_rows: Cap on tree rows loaded for local development. None = full dataset.
        require_postgis: If True, raise on PostGIS write failure instead of logging a
            warning. Set by the Airflow callable so the task fails visibly rather than
            silently succeeding with no data written.
    """
    # 1. Load CSV
    df = load_tree_csv(RAW_CSV, max_rows=max_rows)

    # 1b. Guarantee all dbt-required columns exist (boroname fallback etc.)
    df = ensure_required_columns(df)

    # 2. Build GeoDataFrame in EPSG:4326
    gdf = build_geodataframe(df)

    # 3. Sample NDVI — must happen BEFORE reprojection to 32618
    gdf = sample_ndvi(gdf, NDVI_TIF)

    # 4. Load census tract polygons (synthetic fallback, EPSG:4326)
    tracts = load_census_tracts()

    # 5. Reproject trees to EPSG:32618
    gdf = reproject_trees(gdf)

    # 6. Spatial join (tracts reprojected inside the function)
    gdf = spatial_join(gdf, tracts)

    # 7. Validate and print summary
    validate(gdf)

    # 8. Write outputs
    write_parquet(gdf, PARQUET_OUT)
    print(f"  Parquet saved : {PARQUET_OUT.resolve()}")

    try:
        write_postgis(gdf, DB_URL)
        print("  PostGIS table : raw.tree_census (replaced)")
    except Exception as exc:
        if require_postgis:
            raise
        log.warning("PostGIS write skipped (is the container running?): %s", exc)
        print("  PostGIS       : SKIPPED -- start PostGIS with: docker-compose up -d postgis")

    print("\nDone.")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as exc:
        log.error("Missing input file: %s", exc)
        log.error("Run fetch_tree_data.py and fetch_ndvi.py first.")
        sys.exit(1)
    except AssertionError as exc:
        log.error("Validation failed: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.error("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)
