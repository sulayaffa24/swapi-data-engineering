import os
from typing import Dict
import logging
from logging import StreamHandler, Formatter

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

from src.extract.swapi_client import get_normalized_planets
from src.transform.transform_data import clean_planets_raw, build_gold_tables
from src.utils.gcs_helper import upload_df_to_gcs
from src.load.load_postgres import get_pg_engine, ensure_schema, load_table_to_postgres
from src.load.load_bigquery import load_gold_layer_to_bigquery

logger = logging.getLogger("swapi_pipeline")
logger.setLevel(logging.INFO)

handler = StreamHandler()
handler.setFormatter(Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)


# --------------- Prefect Tasks -------------- #

@task
def extract_bronze() -> pd.DataFrame:
    """Extract raw planets data from SWAPI (Bronze)"""
    pl = get_run_logger()
    pl.info("Extracting Bronze data from SWAPI...")
    df_bronze = get_normalized_planets()
    pl.info(f"Bronze extraction complete. Rows: {len(df_bronze)}")
    return df_bronze

@task
def transform_to_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """Clean and transformed planets data (Silver)"""
    pl = get_run_logger()
    pl.info("Starting SIlver transformation...")
    df_silver = clean_planets_raw(df_bronze)
    pl.info(f"Silver transformation complete. Rows: {len(df_silver)}")
    return df_silver

@task
def transform_to_gold(df_silver: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Build Gold Tables: planets + relationship tables."""
    pl = get_run_logger()
    pl.info("Building Gold data models...")
    gold_tables = build_gold_tables(df_silver)
    pl.info(f"Gold tables created {list(gold_tables.keys())}")
    return gold_tables

@task
def write_layers_to_gcs(
    df_bronze: pd.DataFrame,
    df_silver: pd.DataFrame,
    gold_tables: Dict[str, pd.DataFrame]
):
    """Write Bronze, Silver, Gold DataFrames to GCS as Parquet"""
    pl = get_run_logger()
    bucket = os.getenv("GCS_BUCKET")
    base = os.getenv("GCS_BASE_PATH")

    # BRONZE
    pl.info("Writing Bronze layer to GCS...")
    upload_df_to_gcs(df_bronze, bucket, f"{base}/bronze/planets_raw.parquet")

    # SILVER
    pl.info("Writing Silver layer to GCS...")
    upload_df_to_gcs(df_silver, bucket, f"{base}/silver/planets_clean.parquet")

    # GOLD
    pl.info("Writing Gold layer tables to GCS...")
    for name, table in gold_tables.items():
        upload_df_to_gcs(table, bucket, f"{base}/gold/{name}.parquet")


@task
def load_gold_to_postgres(gold_tables: Dict[str, pd.DataFrame]):
    """Load Gold tables into Postgres from DataFrames."""

    pl = get_run_logger()

    pl.info("Loading Gold tables into Postgres...")
    engine = get_pg_engine()
    schema = os.getenv("POSTGRES_SCHEMA")

    ensure_schema(engine, schema_name=schema)

    for name, table in gold_tables.items():
        pl.info(f"Loading Postgres table: {schema}.{name}")
        load_table_to_postgres(
            df=table,
            table_name=name,
            engine=engine,
            schema=schema,
            if_exists="replace"
        )
    pl.info("Postgres loading complete.")

@task
def load_gold_to_bigquery():
    """Load Gold tables into BigQuery from GCS parquet"""
    pl = get_run_logger()
    pl.info("Loading Gold tables into BigQuery from GCS...")
    load_gold_layer_to_bigquery()
    pl.info("BigQuery loading complete.")


# ---------------- Prefect Flow --------------- #

@flow(name="SWAPI End-to-End Pipeline")
def swapi_pipeine(run_postgres: bool = True, run_bigquery: bool = True, write_gcs: bool = True):
    """
    End-to-end SWAPI pipeline:
    - Extract (Bronze) from SWAPI
    - Transform to Silver and Gold
    - Optionally write Bronze/Silver/Gold to GCS
    - Optionally load Gold to Postgres and/or Bigquery
    """
    logger.info("SWAPI pipeline execution started")
    load_dotenv()

    # Extract
    df_bronze = extract_bronze()

    # Transform
    df_silver = transform_to_silver(df_bronze)
    gold_tables = transform_to_gold(df_silver)

    # Write to GCS
    if write_gcs:
        write_layers_to_gcs(df_bronze, df_silver, gold_tables)

    if run_postgres:
        load_gold_to_postgres(gold_tables)

    if run_bigquery:
        load_gold_to_bigquery()

    logger.info("SWAPI pipeline execution completed successfully!")


if __name__ == "__main__":
    # Run pipeline
    swapi_pipeine(
        run_postgres=True,
        run_bigquery=True,
        write_gcs=True
    )


