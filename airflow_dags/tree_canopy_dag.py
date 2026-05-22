"""
Urban Tree Canopy Pipeline DAG
================================
Orchestrates the full pipeline daily:
  fetch_tree_data → fetch_ndvi → process_and_load
      → dbt_run → dbt_test → export_parquet

Runs inside the treecanopy-airflow Docker container where:
  /opt/airflow/ingestion  → ./ingestion  (host mount)
  /opt/airflow/dbt_project → ./dbt_project (host mount)
  /opt/airflow/data       → ./data       (host mount)

Airflow 2 → 3 migration notes
------------------------------
This DAG is written to be compatible with both Airflow 2 and Airflow 3:
  - schedule="@daily" (not schedule_interval) — valid from Airflow 2.4, unchanged in 3.
  - catchup=False — default in Airflow 3; explicit here for clarity.
  - provide_context=True removed — context is injected automatically in both versions.
  - on_failure_callback uses logical_date (Airflow 3 name) with execution_date fallback
    for Airflow 2 compatibility; execution_date is removed in Airflow 3.
  - doc_md=__doc__ — valid in both versions.
  - No SubDagOperator (removed in Airflow 3).
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make the ingestion package importable when Airflow imports this DAG file.
# /opt/airflow is the container working root; ingestion/ is mounted there.
_AIRFLOW_HOME = Path("/opt/airflow")
if str(_AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(_AIRFLOW_HOME))

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = _AIRFLOW_HOME / "data"
LOGS_DIR = DATA_DIR / "logs"
PROCESSED_DIR = DATA_DIR / "processed"
DBT_DIR = _AIRFLOW_HOME / "dbt_project"

# Reads TREECANOPY_DB_URL set in docker-compose.yml; falls back to localhost
# so the same process_and_load.py works for local runs too.
DB_URL = os.environ.get(
    "TREECANOPY_DB_URL",
    "postgresql+psycopg2://danish:danish123@postgis:5432/treecanopy",
)

DBT_CMD_BASE = (
    f"dbt {{verb}}"
    f" --project-dir {DBT_DIR}"
    f" --profiles-dir {DBT_DIR}"
)


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------
def on_failure_callback(context: dict) -> None:
    """Append a one-line failure record to data/logs/failures.log.

    Args:
        context: Airflow task context dict provided automatically on failure.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "failures.log"

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    # logical_date is the Airflow 3 name; execution_date is the Airflow 2 alias
    execution_date = context.get("logical_date", context.get("execution_date", "unknown"))
    exception = context.get("exception", "no exception info")

    entry = (
        f"[{datetime.now(timezone.utc).isoformat()}] "
        f"dag={dag_id} task={task_id} "
        f"exec_date={execution_date} "
        f"error={exception}\n"
    )
    with log_file.open("a") as fh:
        fh.write(entry)
    log.error("Pipeline failure logged: %s", entry.strip())


# ---------------------------------------------------------------------------
# Python callables
# ---------------------------------------------------------------------------
def fetch_tree_data_callable() -> None:
    """Download the NYC 2015 Street Tree Census CSV (idempotent — skips if exists)."""
    from ingestion.fetch_tree_data import main
    main()


def fetch_ndvi_callable() -> None:
    """Generate the synthetic NDVI GeoTIFF (idempotent — skips if exists)."""
    from ingestion.fetch_ndvi import main
    main()


def process_and_load_callable() -> None:
    """Run the full geospatial processing pipeline and write raw.tree_census.

    Uses TREECANOPY_DB_URL from the environment to connect to PostGIS,
    so 'postgis' (Docker service name) is resolved correctly inside the container.
    require_postgis=True makes the task fail visibly if the PostGIS write fails,
    rather than silently succeeding with no data written.
    """
    from ingestion.process_and_load import main
    main(require_postgis=True)


def export_parquet_callable() -> None:
    """Read analytics.mart_canopy_coverage from PostGIS and write to Parquet.

    Output: data/processed/mart_canopy_coverage.parquet
    Overwrites any existing file so reruns are idempotent.

    Uses SQLAlchemy 2.x execute() + fetchall() directly instead of
    pd.read_sql_query(), which has a version-dependent routing bug where it
    treats a SQLAlchemy Connection as a DBAPI2 object and calls .cursor() on
    it, causing AttributeError in the pandas/SQLAlchemy versions shipped in
    the Airflow 2.9.1 container.
    """
    import pandas as pd
    from sqlalchemy import create_engine, text

    out_path = PROCESSED_DIR / "mart_canopy_coverage.parquet"

    log.info("Connecting to PostGIS: %s", DB_URL.split("@")[-1])
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM analytics.mart_canopy_coverage"))
        df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))

    log.info("Fetched %d rows from analytics.mart_canopy_coverage", len(df))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    log.info("Parquet written to %s", out_path)
    print(f"  Exported {len(df):,} rows -> {out_path}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="urban_tree_canopy_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["geospatial", "tree_canopy", "dbt"],
    default_args={
        "on_failure_callback": on_failure_callback,
        "retries": 0,
    },
    doc_md=__doc__,
) as dag:

    fetch_tree_data = PythonOperator(
        task_id="fetch_tree_data",
        python_callable=fetch_tree_data_callable,
    )

    fetch_ndvi = PythonOperator(
        task_id="fetch_ndvi",
        python_callable=fetch_ndvi_callable,
    )

    process_and_load = PythonOperator(
        task_id="process_and_load",
        python_callable=process_and_load_callable,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_CMD_BASE.format(verb="run"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD_BASE.format(verb="test"),
    )

    export_parquet = PythonOperator(
        task_id="export_parquet",
        python_callable=export_parquet_callable,
    )

    # fetch_tree_data and fetch_ndvi are independent — run them in parallel
    [fetch_tree_data, fetch_ndvi] >> process_and_load >> dbt_run >> dbt_test >> export_parquet
