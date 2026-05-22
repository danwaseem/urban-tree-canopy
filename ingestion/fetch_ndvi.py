"""
Generate a synthetic NDVI GeoTIFF in EPSG:4326 covering the NYC bounding box.

The raster stays in EPSG:4326 so that tree point coordinates (also 4326) can
be sampled directly before any reprojection to a projected CRS (e.g. EPSG:32618).
"""

import logging
import sys
from pathlib import Path

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# NYC bounding box in EPSG:4326
LON_MIN, LON_MAX = -74.26, -73.70
LAT_MIN, LAT_MAX = 40.49, 40.92
RESOLUTION = 0.001  # degrees per pixel

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_PATH = RAW_DIR / "nyc_ndvi_synthetic.tif"


def generate_ndvi_raster(dest: Path) -> tuple[int, int]:
    """Create a synthetic single-band NDVI GeoTIFF in EPSG:4326.

    Values are random float32 in [-0.2, 0.9] -- a plausible range for urban
    NDVI where negative values represent water/pavement and high values dense
    vegetation.

    Args:
        dest: Output path for the GeoTIFF.

    Returns:
        (height, width) pixel dimensions of the written raster.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    width = int(round((LON_MAX - LON_MIN) / RESOLUTION))
    height = int(round((LAT_MAX - LAT_MIN) / RESOLUTION))
    log.info("Raster dimensions: %d rows x %d cols (%.4f deg/px)", height, width, RESOLUTION)

    transform = from_bounds(LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, width, height)
    log.info("Affine transform: %s", transform)

    rng = np.random.default_rng(seed=42)
    ndvi = rng.uniform(-0.2, 0.9, size=(height, width)).astype(np.float32)
    log.info("NDVI array generated -- min=%.4f  max=%.4f", ndvi.min(), ndvi.max())

    crs = CRS.from_epsg(4326)

    log.info("Writing GeoTIFF to %s", dest)
    with rasterio.open(
        dest,
        mode="w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=np.float32,
        crs=crs,
        transform=transform,
        compress="lzw",
        nodata=-9999.0,
    ) as ds:
        ds.write(ndvi, 1)

    return height, width


def summarise_raster(path: Path) -> None:
    """Open the written raster and log key metadata.

    Args:
        path: Path to the GeoTIFF to inspect.
    """
    with rasterio.open(path) as ds:
        data = ds.read(1)
        log.info(
            "Verified raster -- shape=%s  CRS=%s  dtype=%s",
            ds.shape,
            ds.crs.to_string(),
            ds.dtypes[0],
        )
        print("\n--- Synthetic NDVI raster: generation complete ---")
        print(f"  Path     : {path.resolve()}")
        print(f"  Shape    : {ds.height} rows x {ds.width} cols")
        print(f"  CRS      : {ds.crs.to_string()}")
        print(f"  Min NDVI : {data.min():.4f}")
        print(f"  Max NDVI : {data.max():.4f}")


def main() -> None:
    if OUTPUT_PATH.exists():
        log.info("File already exists at %s -- skipping generation", OUTPUT_PATH)
    else:
        generate_ndvi_raster(OUTPUT_PATH)

    summarise_raster(OUTPUT_PATH)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)
