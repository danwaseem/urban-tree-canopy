"""
Spatial integrity tests for the urban-tree-canopy pipeline.

Validates the two Parquet outputs produced by process_and_load.py and the
dbt export step:
  - data/processed/tree_census_processed.parquet  (row-level processed data)
  - data/processed/mart_canopy_coverage.parquet   (aggregated mart)

Run:
    pytest tests/ -v
"""

from pathlib import Path

import pandas as pd
import pytest
from shapely import wkb

BASE_DIR = Path(__file__).parent.parent
PROCESSED_PARQUET = BASE_DIR / "data" / "processed" / "tree_census_processed.parquet"
CANOPY_PARQUET = BASE_DIR / "data" / "processed" / "mart_canopy_coverage.parquet"

# ---------------------------------------------------------------------------
# Fixtures — loaded once per session so each test doesn't re-read from disk
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def processed_df() -> pd.DataFrame:
    return pd.read_parquet(PROCESSED_PARQUET)


@pytest.fixture(scope="session")
def canopy_df() -> pd.DataFrame:
    return pd.read_parquet(CANOPY_PARQUET)


# ---------------------------------------------------------------------------
# File existence
# ---------------------------------------------------------------------------

def test_processed_parquet_exists() -> None:
    """Ingestion pipeline must have written the processed Parquet."""
    assert PROCESSED_PARQUET.exists(), (
        f"Missing: {PROCESSED_PARQUET}\n"
        "Run: python ingestion/process_and_load.py"
    )


def test_canopy_parquet_exists() -> None:
    """dbt export step must have written the mart Parquet."""
    assert CANOPY_PARQUET.exists(), (
        f"Missing: {CANOPY_PARQUET}\n"
        "Run: python queries/duckdb_analysis.py  (refreshes from PostGIS)"
    )


# ---------------------------------------------------------------------------
# Coordinate integrity
# ---------------------------------------------------------------------------

def test_no_null_coordinates(processed_df: pd.DataFrame) -> None:
    """Every tree must have a valid WGS-84 latitude and longitude."""
    null_lat = processed_df["latitude"].isna().sum()
    null_lon = processed_df["longitude"].isna().sum()
    assert null_lat == 0, f"{null_lat} rows have null latitude"
    assert null_lon == 0, f"{null_lon} rows have null longitude"


def test_projected_coordinates_exist(processed_df: pd.DataFrame) -> None:
    """x_32618 / y_32618 columns must exist and contain UTM metre values.

    NYC in UTM Zone 18N:
      easting  (x) ≈ 550 000 – 650 000 m
      northing (y) ≈ 4 470 000 – 4 620 000 m
    Degree-range values (-180 to 180) would indicate the geometry was never
    reprojected from EPSG:4326.
    """
    assert "x_32618" in processed_df.columns, "x_32618 column missing"
    assert "y_32618" in processed_df.columns, "y_32618 column missing"

    # All values must be finite
    assert processed_df["x_32618"].notna().all(), "x_32618 contains nulls"
    assert processed_df["y_32618"].notna().all(), "y_32618 contains nulls"

    # Sanity-check range: UTM metres, not degrees
    assert processed_df["x_32618"].abs().max() > 10_000, (
        "x_32618 values look like degrees, not UTM metres"
    )
    assert processed_df["y_32618"].abs().max() > 10_000, (
        "y_32618 values look like degrees, not UTM metres"
    )

    # Tighter NYC-specific bounds
    x_min, x_max = processed_df["x_32618"].min(), processed_df["x_32618"].max()
    y_min, y_max = processed_df["y_32618"].min(), processed_df["y_32618"].max()
    assert 500_000 < x_min and x_max < 700_000, (
        f"x_32618 out of NYC UTM range: [{x_min:.0f}, {x_max:.0f}]"
    )
    assert 4_400_000 < y_min and y_max < 4_700_000, (
        f"y_32618 out of NYC UTM range: [{y_min:.0f}, {y_max:.0f}]"
    )


# ---------------------------------------------------------------------------
# NDVI integrity
# ---------------------------------------------------------------------------

def test_ndvi_range(processed_df: pd.DataFrame) -> None:
    """All sampled NDVI values must fall in the valid [-1.0, 1.0] range."""
    ndvi = processed_df["ndvi_value"].dropna()
    assert len(ndvi) > 0, "ndvi_value column is entirely null"
    out_of_range = ((ndvi < -1.0) | (ndvi > 1.0)).sum()
    assert out_of_range == 0, (
        f"{out_of_range} ndvi_value entries outside [-1.0, 1.0]"
    )


# ---------------------------------------------------------------------------
# Mart integrity
# ---------------------------------------------------------------------------

def test_species_count_positive(canopy_df: pd.DataFrame) -> None:
    """Every census tract must have at least one identified species."""
    col = canopy_df["species_count"].dropna()
    assert len(col) > 0, "species_count column is entirely null"
    non_positive = (col <= 0).sum()
    assert non_positive == 0, (
        f"{non_positive} rows have species_count <= 0"
    )


def test_tree_count_positive(canopy_df: pd.DataFrame) -> None:
    """Every census tract row must contain at least one tree."""
    col = canopy_df["tree_count"].dropna()
    assert len(col) > 0, "tree_count column is entirely null"
    non_positive = (col <= 0).sum()
    assert non_positive == 0, (
        f"{non_positive} rows have tree_count <= 0"
    )


def test_geometry_wkb_valid(processed_df: pd.DataFrame) -> None:
    """geometry_wkb column must contain parseable, valid Point geometries.

    The WKB bytes are written by process_and_load.py after reprojection to
    EPSG:32618. Parsing them confirms the geometry column survived the
    Parquet round-trip and that no WKB corruption occurred.
    """
    assert "geometry_wkb" in processed_df.columns, "geometry_wkb column missing"

    sample = processed_df["geometry_wkb"].dropna().head(500)
    assert len(sample) > 0, "geometry_wkb column is entirely null"

    invalid = 0
    for raw in sample:
        try:
            geom = wkb.loads(bytes(raw))
            if not geom.is_valid or geom.geom_type != "Point":
                invalid += 1
        except Exception:
            invalid += 1

    assert invalid == 0, f"{invalid}/500 sampled WKB geometries are invalid or non-Point"


def test_boroname_values(processed_df: pd.DataFrame) -> None:
    """Processed data must contain at least four of the five NYC boroughs.

    Guards against the boroname derivation silently falling back to UNKNOWN
    for all rows (happened when the 'borough' CSV column was missing from
    KEEP_COLS).
    """
    known_boroughs = {"Queens", "Brooklyn", "Manhattan", "Bronx", "Staten Island"}
    found = set(processed_df["boroname"].dropna().unique())
    matched = known_boroughs & found
    assert len(matched) >= 4, (
        f"Expected ≥4 NYC boroughs in boroname, found: {found}"
    )


def test_no_duplicate_tree_ids(processed_df: pd.DataFrame) -> None:
    """tree_id must be unique across all processed rows."""
    duplicates = processed_df["tree_id"].duplicated().sum()
    assert duplicates == 0, f"{duplicates} duplicate tree_id values found"
