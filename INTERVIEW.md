# Interview Study Guide — Urban Tree Canopy Pipeline

---

## 30-Second Project Pitch

> "I built a production-style geospatial ELT pipeline over the NYC 2015 Street Tree Census. It ingests 100 000 street trees, samples NDVI values from a raster, reprojects coordinates from WGS-84 to UTM, and spatial-joins each tree to a census tract polygon — all written to PostGIS. From there, dbt models clean and aggregate the data into analytical marts, which are exported to Parquet and queried with DuckDB in the same pattern used by AWS Athena. The whole pipeline runs as an Airflow DAG with 38 dbt data quality tests and a GitHub Actions CI gate."

---

## Architecture — How to Explain Each Layer

### Ingestion
- `fetch_tree_data.py` streams the NYC Open Data CSV in 1 MB chunks — idempotent (skips if file exists)
- `fetch_ndvi.py` generates a synthetic single-band NDVI GeoTIFF in EPSG:4326 using NumPy (seeded RNG for reproducibility)
- In production, NDVI would come from a satellite provider (Sentinel-2, Landsat) or a pre-computed raster stored in S3

### Processing (`process_and_load.py`) — the core spatial work
Eight ordered steps:
1. Load CSV, normalise column names, drop rows with null lat/lon
2. Derive `boroname` from `borough` column (with borocode fallback)
3. Build GeoDataFrame — `Point(longitude, latitude)` in **EPSG:4326**
4. Sample NDVI from raster — **must happen while still in EPSG:4326** (raster CRS = 4326)
5. Build synthetic census tract grid in EPSG:4326
6. Reproject trees to **EPSG:32618** (UTM Zone 18N) — metre-based, good for NYC distances
7. Spatial join — `gpd.sjoin(trees, tracts, how='left', predicate='within')`
8. Validate → write Parquet + PostGIS

### dbt
- `stg_tree_census` — **view**, cleans and casts the raw table, filters `status = 'alive'`
- `mart_canopy_coverage` — **table**, one row per (census_tract_id, boroname)
- `mart_species_health` — **table**, one row per (spc_common, health)
- 38 tests across source, staging, and marts

### Analytics
- `duckdb_analysis.py` refreshes the mart Parquet from PostGIS then runs SQL with DuckDB
- DuckDB SQL is Presto/Trino-compatible — the same queries run unchanged on AWS Athena
- Parquet is "uploaded" via `shutil.copy2` to `data/mock_s3/` — one line change swaps this for `boto3.client('s3').upload_file()`

---

## Key Concepts You Must Be Able to Explain

### CRS — Why two projections?

| CRS | EPSG | Unit | Used for |
|---|---|---|---|
| WGS-84 | 4326 | Degrees | Raw lat/lon, NDVI raster sampling |
| UTM Zone 18N | 32618 | Metres | Spatial joins, distance calculations |

**Why sample NDVI in 4326?** The raster is stored in EPSG:4326. If you reproject the points first, the coordinates no longer align with the raster pixels — you'd sample wrong values or get NaN. The assert statement in `sample_ndvi()` enforces this ordering:
```python
assert str(gdf.crs.to_epsg()) == "4326", "Sample NDVI before reprojecting to 32618!"
```

**Why reproject to 32618 for the spatial join?** GeoPandas `sjoin` uses Shapely geometry predicates (`within`, `intersects`). These are computed in the coordinate units of the GeoDataFrame. In degrees, a 0.001° difference means different distances at different latitudes (the Earth is not a perfect sphere). In UTM metres, 1 unit = 1 metre everywhere in the zone — spatial relationships are accurate.

### NDVI
- Normalized Difference Vegetation Index — ranges from -1 to 1
- Negative / near-zero: water, pavement, buildings
- 0.2–0.5: sparse vegetation
- 0.5–0.9: dense vegetation
- Computed from satellite bands: `(NIR - Red) / (NIR + Red)`
- In this project: synthetic values in `[-0.2, 0.9]` — realistic urban range

### PostGIS
- PostgreSQL extension that adds spatial column types and functions
- `raw.tree_census` stores trees as plain numeric columns (lat, lon, x_32618, y_32618)
- In production you'd use `geometry(POINT, 32618)` PostGIS type to enable spatial indexing and SQL functions like `ST_Distance`, `ST_Within`
- We write with plain `df.to_sql()` rather than `gdf.to_postgis()` to avoid GeoAlchemy2 version sensitivity between Python 3.11 (local) and 3.12 (Airflow container)

### Spatial Join
```python
gpd.sjoin(trees, tracts, how='left', predicate='within')
```
- `how='left'` — every tree is kept; unmatched trees get `census_tract_id = 'UNKNOWN'`
- `predicate='within'` — tree point must fall strictly inside the tract polygon
- Returns a copy of trees with `census_tract_id` added from the matching polygon
- `index_right` is a sjoin artifact — dropped immediately after

### dbt Model Types
- **View** (`stg_tree_census`) — no data stored in dbt, query runs against PostGIS live. Cheap to rebuild, always reflects current source data.
- **Table** (`mart_canopy_coverage`, `mart_species_health`) — data materialised into PostGIS at `dbt run` time. Faster to query, decoupled from source changes.

### dbt Tests — 38 total
**Built-in generic tests:**
- `not_null` — column has no NULL values
- `unique` — no duplicate values (tree_id in staging and source)
- `accepted_values` — all values are in a known set (boroname, health, status)

**Custom generic tests (in `macros/generic_tests.sql`):**
- `value_between(min, max)` — used for NDVI [-1,1], tree_dbh [0,450], UTM easting [500k,700k], UTM northing [4.4M,4.7M]
- `positive_value` — strictly > 0 (tree_count, species_count)

**Source test:**
- `freshness` — warns if `loaded_at` is older than 24h, errors after 48h. Catches stale data before dbt runs.

### Parquet + DuckDB / Athena
- Parquet is columnar — scanning `avg_ndvi` reads only that column, not all rows
- DuckDB reads Parquet directly from disk (zero-copy via Arrow)
- AWS Athena also reads Parquet from S3 using the same Presto/Trino SQL engine
- The mock: `shutil.copy2(src, data/mock_s3/...)` — in production: `boto3.client('s3').upload_file(...)`

### Airflow DAG
- `schedule="@daily"`, `catchup=False` — runs once per day, no backfill
- `[fetch_tree_data, fetch_ndvi] >> process_and_load` — the two fetches run **in parallel** (independent tasks)
- `BashOperator` for dbt (runs in shell), `PythonOperator` for Python callables
- `on_failure_callback` — appends structured failure records to `data/logs/failures.log`
- `require_postgis=True` in the DAG callable — ensures PostGIS write failures surface as red tasks, not silent successes

---

## Bugs You Triaged (Say These Confidently)

### 1. Silent PostGIS write failure in Airflow
**Symptom:** `process_and_load` task showed green (success), but `dbt_run` failed with `relation "raw.tree_census" does not exist`.

**Root cause:** `gdf.to_postgis()` internally uses GeoAlchemy2's geometry type system. GeoAlchemy2 >= 0.14 changed the geometry type string format, breaking on Python 3.12 in the Airflow container with `geometry (geometry(POINT,32618)) not a string`. The exception was caught by a broad `except` clause, so the task exited 0. But the `DROP TABLE CASCADE` had already run — leaving no table.

**Fix 1:** Switched from `gdf.to_postgis()` to `df.to_sql()` (plain pandas, no GeoAlchemy2 geometry type). Spatial data preserved in `latitude`, `longitude`, `x_32618`, `y_32618` columns.

**Fix 2:** Added `require_postgis=True` parameter to `main()` — when called from Airflow, PostGIS failures re-raise instead of being swallowed. Task turns red immediately.

### 2. `AttributeError: 'Engine'/'Connection' object has no attribute 'cursor'`
**Symptom:** `df.to_sql(con=engine, ...)` failed in the Airflow container (Python 3.12).

**Root cause:** pandas in the Airflow container (Python 3.12) does not recognise SQLAlchemy 2.x `Engine` or `Connection` objects — it falls through to the DBAPI2 code path and calls `.cursor()`, which does not exist on either object.

**Fix:** Bypassed `df.to_sql` entirely. Used `engine.raw_connection()` to get a raw psycopg2 connection, serialised the DataFrame to a CSV buffer with `df.to_csv()`, and bulk-loaded via psycopg2's `copy_expert`. This is version-agnostic and also faster than row-by-row inserts.

### 3. `boroname = UNKNOWN` for all rows
**Symptom:** DuckDB output showed `UNKNOWN` for every borough.

**Root cause:** `KEEP_COLS` included `'boroname'` but the NYC CSV column is named `'borough'`. The column was silently dropped, triggering the fallback to `'UNKNOWN'`.

**Fix:** Changed `KEEP_COLS` to include `'borough'` and derived `boroname` from it: `df["boroname"] = df["borough"]`.

### 4. `cannot drop table raw.tree_census because other objects depend on it`
**Symptom:** Second and subsequent runs of `process_and_load.py` failed with a PostgreSQL dependency error.

**Root cause:** `gdf.to_postgis(if_exists='replace')` internally runs `DROP TABLE` without `CASCADE`. The `analytics.stg_tree_census` view depends on `raw.tree_census`, so PostgreSQL blocks the drop.

**Fix:** Explicitly run `DROP TABLE IF EXISTS raw.tree_census CASCADE` in a separate transaction before writing.

---

## Expected Interview Questions

**Q: Why EPSG:32618 instead of keeping everything in 4326?**
A: GeoPandas spatial predicates (`within`, `intersects`) compute correctly when coordinates are in metres. In degrees, a 0.0001° difference near the equator vs near the poles represents very different real-world distances. UTM Zone 18N covers NYC exactly and gives metre-level accuracy.

**Q: What is NDVI and why does the CRS matter for sampling it?**
A: NDVI is a vegetation index derived from satellite NIR and red bands. The raster is stored in EPSG:4326 (pixel = degrees of lat/lon). If you reproject the tree points to 32618 first, the (x,y) values become metres — they no longer map to raster pixel coordinates, so `rasterio.sample()` returns wrong values or NaN.

**Q: Why is `stg_tree_census` a view and the marts are tables?**
A: The staging model is a thin cleaning layer — cheap to recompute and should always reflect the latest source data. The mart tables are pre-aggregated to 68 and 357 rows respectively; materialising them means downstream queries don't re-scan 100k rows every time.

**Q: How do you enforce data quality in this pipeline?**
A: Three layers. First, Python `assert` statements in `validate()` check geometry validity, CRS, and NDVI range before writing to PostGIS. Second, dbt runs 38 tests after every `dbt run` — not_null, unique, accepted_values for boroughs and health ratings, value_between for NDVI and UTM coordinates. Third, pytest runs 10 spatial integrity checks on the Parquet outputs — including WKB geometry parsing and boroname coverage.

**Q: How does the Athena querying work?**
A: DuckDB reads Parquet directly with `read_parquet()` and its SQL dialect is Presto/Trino-compatible. In production, the Parquet files would live in S3 partitioned by borough or date, and you'd point an Athena table definition at the S3 prefix. The SQL queries are identical — no code changes needed.

**Q: How did you handle the Airflow 2 → 3 migration?**
A: The DAG was written to be compatible with both. Key changes: `schedule_interval` → `schedule` (done from the start), `execution_date` → `logical_date` in the failure callback with a fallback for Airflow 2, `provide_context` removed (not needed since Airflow 2.0), no `SubDagOperator` (removed in Airflow 3). All compatibility decisions are documented in the DAG docstring.

**Q: What would you change for a production deployment?**
A:
- Replace synthetic census tract grid with real Census TIGER boundaries
- Replace synthetic NDVI GeoTIFF with real satellite-derived rasters (Sentinel-2 via Google Earth Engine or AWS)
- Add a spatial index on `raw.tree_census` geometry column for fast PostGIS queries
- Partition the S3 Parquet by `boroname` and `loaded_at` date for Athena partition pruning
- Store credentials in AWS Secrets Manager / Airflow Connections instead of hardcoded strings
- Enable dbt source freshness alerting to PagerDuty or Slack
- Add row-count reconciliation between raw PostGIS and mart Parquet

**Q: What does DataHub provide here?**
A: DataHub tracks lineage — which datasets produced which other datasets. `datahub/lineage_manifest.yml` documents the full graph: CSV + GeoTIFF → PostGIS raw → dbt staging → dbt marts → S3 Parquet. With a live DataHub instance, `datahub/recipe.yml` would crawl the PostGIS schemas and emit column-level descriptions and ownership metadata. This means a data consumer can look at `mart_canopy_coverage` in DataHub and trace every column back to its source.

---

## Numbers to Remember

| Metric | Value |
|---|---|
| Full NYC dataset | 488 699 trees |
| Loaded for local dev | 100 000 trees |
| Census tracts (synthetic) | 58 matched (of 108 grid cells) |
| dbt models | 3 (1 view + 2 tables) |
| dbt tests | 38 |
| pytest tests | 10 |
| mart_canopy_coverage rows | 68 (tract × borough pairs) |
| mart_species_health rows | 357 (species × health pairs) |
| NDVI range (synthetic) | −0.20 to 0.90 |
| Bronx mean NDVI | 0.39 (highest — large parks) |
| Brooklyn mean NDVI | 0.32 (lowest — dense urban) |
