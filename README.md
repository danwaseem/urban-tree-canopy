# Urban Tree Canopy — Geospatial Data Pipeline

End-to-end geospatial ELT pipeline over the NYC 2015 Street Tree Census. Ingests 100 000 street trees, samples synthetic NDVI from a GeoTIFF raster, reprojects to UTM, spatial-joins each tree to a census tract, loads into PostGIS, transforms with dbt, and queries with DuckDB in the same pattern used by AWS Athena.

---

## Architecture

```
NYC Tree Census CSV          Synthetic NDVI GeoTIFF
(488k trees, 45 cols)        (EPSG:4326, 430×560px)
        │                             │
  fetch_tree_data.py           fetch_ndvi.py
        └──────────┬──────────────────┘
                   ▼
          process_and_load.py
          ├─ Build GeoDataFrame      EPSG:4326
          ├─ Sample NDVI at points   EPSG:4326  ← must happen before reproject
          ├─ Reproject               EPSG:4326 → EPSG:32618 (UTM Zone 18N)
          ├─ Spatial join            trees within census tract polygons
          ├─ Write Parquet           WKB geometry + x_32618/y_32618
          └─ Write PostGIS           raw.tree_census
                   │
                 dbt run
          ├─ analytics.stg_tree_census      (view  — clean, cast, filter alive)
          ├─ analytics.mart_canopy_coverage (table — tract × borough aggregates)
          └─ analytics.mart_species_health  (table — species × health aggregates)
                 dbt test  →  38 tests: not_null, unique, accepted_values,
                               value_between, positive_value, source freshness
                   │
          export_parquet  →  data/processed/mart_canopy_coverage.parquet
                   │
              DuckDB / Athena
          ├─ Top-10 tracts by canopy density
          ├─ Mean NDVI by borough
          └─ Species richness vs tree count
                   │
          data/mock_s3/marts/   ←  S3 upload (mocked locally via shutil)
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Spatial database | PostGIS 3.4 (PostgreSQL 15) |
| Spatial processing | GeoPandas, Shapely, Rasterio |
| Transformation | dbt-postgres 1.11 |
| Columnar storage | Apache Parquet (PyArrow) |
| Analytics | DuckDB (Athena-compatible SQL) |
| Orchestration | Apache Airflow 2.9.1 |
| Cloud storage | boto3 pattern — mocked locally |
| Lineage | DataHub recipe + lineage manifest |
| CI/CD | GitHub Actions — dbt test gate |
| Testing | pytest (10 spatial integrity tests) |

---

## Project Structure

```
urban-tree-canopy/
├── ingestion/
│   ├── fetch_tree_data.py      stream NYC Open Data CSV
│   ├── fetch_ndvi.py           generate synthetic NDVI GeoTIFF
│   └── process_and_load.py     spatial pipeline → PostGIS + Parquet
├── dbt_project/
│   ├── models/
│   │   ├── staging/            stg_tree_census view + sources + schema tests
│   │   └── marts/              mart_canopy_coverage, mart_species_health
│   ├── macros/                 value_between, positive_value custom tests
│   └── profiles.yml            reads POSTGIS_HOST env var
├── airflow_dags/
│   └── tree_canopy_dag.py      daily DAG — parallel fetch → load → dbt → export
├── queries/
│   └── duckdb_analysis.py      refresh Parquet from PostGIS → DuckDB queries
├── tests/
│   └── test_spatial_integrity.py   10 pytest checks on Parquet outputs
├── datahub/
│   ├── recipe.yml              DataHub PostgreSQL ingestion recipe
│   └── lineage_manifest.yml    full URN lineage graph
├── .github/workflows/
│   └── ci.yml                  GitHub Actions — PostGIS service + dbt test gate
├── docker-compose.yml          postgis + airflow services
└── requirements.txt
```

---

## Quickstart

### 1. Start PostGIS

```bash
docker-compose up -d postgis
docker-compose ps          # wait for Status: healthy
```

### 2. Set up Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Fetch source data

```bash
python ingestion/fetch_tree_data.py   # streams NYC CSV (~220 MB), skips if exists
python ingestion/fetch_ndvi.py        # generates synthetic NDVI GeoTIFF, skips if exists
```

### 4. Process and load

```bash
python ingestion/process_and_load.py
```

Expected output:
```
All validation checks passed
Total trees          : 100,000
Trees with NDVI      : 100,000
Unique census tracts : 58
CRS                  : EPSG:32618
PostGIS table        : raw.tree_census (replaced)
```

### 5. dbt

```bash
cd dbt_project
dbt debug --profiles-dir .    # verify connection
dbt run   --profiles-dir .    # PASS=3
dbt test  --profiles-dir .    # PASS=38
cd ..
```

### 6. DuckDB analytics

```bash
python queries/duckdb_analysis.py
```

Refreshes `mart_canopy_coverage.parquet` from PostGIS, runs three queries, writes `data/output/duckdb_results.csv`, mocks an S3 upload.

### 7. Spatial integrity tests

```bash
pytest tests/ -v              # 10 passed
```

---

## Running with Airflow

```bash
docker-compose up -d
docker-compose logs -f airflow   # wait for "Airflow is ready"
open http://localhost:8080
```

Credentials are printed in the Airflow container logs on first startup:
```
standalone | Login with username: admin  password: <generated>
```

Toggle on `urban_tree_canopy_pipeline` and click **▶ Trigger DAG**. The graph view shows:

```
fetch_tree_data ─┐
                 ├──▶ process_and_load ──▶ dbt_run ──▶ dbt_test ──▶ export_parquet
fetch_ndvi      ─┘
```

---

## DataHub Lineage

`datahub/recipe.yml` — PostgreSQL ingestion recipe (run with `datahub ingest -c datahub/recipe.yml` against a live DataHub instance).

`datahub/lineage_manifest.yml` — full URN lineage graph from source CSV through PostGIS, dbt staging, dbt marts, to S3 Parquet.

---

## CI/CD

`.github/workflows/ci.yml` runs on every push and pull request to `main`:

1. Spins up PostGIS as a service container
2. Generates 2 000 synthetic trees (no network download)
3. Runs the full pipeline
4. **Gates on `dbt test`** — non-zero exit fails the build
5. Runs `pytest tests/ -v`

---

## Troubleshooting

**PostGIS write blocked by dependent view**
`process_and_load.py` drops `raw.tree_census` with `CASCADE` before reloading, which removes the `stg_tree_census` dependent view. Run `process_and_load.py` first, then `dbt run`.

**`df.to_sql` AttributeError: 'Connection' has no attribute 'cursor'**
Pass `engine` (not `conn`) to pandas `to_sql`. pandas 2.x with SQLAlchemy 2.x treats a `Connection` object as DBAPI2 and calls `.cursor()`, which does not exist on SQLAlchemy connections.

**Airflow UI not reachable after `docker-compose up`**
First run installs geospatial packages before starting the webserver — takes 2–3 minutes. Watch `docker-compose logs -f airflow` for `Airflow is ready`.

**DuckDB shows UNKNOWN for boroname**
The mart Parquet is stale. `duckdb_analysis.py` will auto-refresh it from PostGIS if the container is running. Otherwise re-run `process_and_load.py` → `dbt run` → `duckdb_analysis.py`.
