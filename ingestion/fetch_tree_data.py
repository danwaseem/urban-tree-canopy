"""
Download the NYC 2015 Street Tree Census CSV and save it to data/raw/.
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

NYC_TREE_CENSUS_URL = (
    "https://data.cityofnewyork.us/api/views/uvpi-gqnh/rows.csv?accessType=DOWNLOAD"
)
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_PATH = RAW_DIR / "nyc_tree_census_2015.csv"


def download_tree_census(url: str, dest: Path, chunk_size: int = 1 << 20) -> None:
    """Stream the tree census CSV from the NYC Open Data portal to *dest*.

    Args:
        url: Full download URL for the CSV.
        dest: Local file path to write to.
        chunk_size: Streaming chunk size in bytes (default 1 MB).
    """
    log.info("Starting download from %s", url)
    dest.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0))
        log.info(
            "Response OK — Content-Length: %s bytes",
            f"{total:,}" if total else "unknown",
        )
        with dest.open("wb") as fh:
            downloaded = 0
            for chunk in resp.iter_content(chunk_size=chunk_size):
                fh.write(chunk)
                downloaded += len(chunk)
        log.info("Wrote %s bytes to %s", f"{downloaded:,}", dest)


def load_and_summarise(path: Path) -> pd.DataFrame:
    """Read the saved CSV and return a DataFrame for quick validation.

    Args:
        path: Path to the saved CSV file.

    Returns:
        DataFrame loaded from *path*.
    """
    log.info("Reading CSV from %s", path)
    df = pd.read_csv(path, low_memory=False)
    log.info("Loaded %d rows x %d columns", len(df), len(df.columns))
    return df


def main() -> None:
    if OUTPUT_PATH.exists():
        log.info("File already exists at %s -- skipping download", OUTPUT_PATH)
    else:
        download_tree_census(NYC_TREE_CENSUS_URL, OUTPUT_PATH)

    df = load_and_summarise(OUTPUT_PATH)

    print("\n--- NYC Tree Census 2015: fetch complete ---")
    print(f"  Rows   : {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Names  : {list(df.columns)}")
    print(f"  Saved  : {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        log.error("HTTP error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.error("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)
