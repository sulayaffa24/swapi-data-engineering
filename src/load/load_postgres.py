import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from src.extract.swapi_client import get_normalized_planets
from src.transform.transform_data import clean_planets_raw
from src.transform.transform_data import build_gold_tables

def get_pg_engine():
    """
    Create a SQLAlchemy engine for the Postgres database.
    Uses environment variables for configuration.
    """
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(url)
    return engine 

def ensure_schema(engine, schema_name: str="swapi"):
    """
    Ensure the target schema exists in Postgres.
    """
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
        conn.commit()


def load_table_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    engine,
    schema: str="swapi",
    if_exists: str="replace"
):
    """
    Load a single DataFrame into Postgres using pandas.to_sql
    """
    print(f"Loading table '{schema}.{table_name}' with {len(df)} rows...")
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False
    )
    print(f"Finished loading '{schema}.{table_name}'")

def main():
    # Load env variables
    load_dotenv()

    # Build engine and ensure schema
    engine = get_pg_engine()
    schema = os.getenv("POSTGRES_SCHEMA")
    ensure_schema(engine, schema_name=schema)

    # Extract and Transform to get Gold DataFrames
    print("Extracting Bronze data from SWAPI...")
    df_bronze = get_normalized_planets()

    print("Transforming to Silver (clean)...")
    df_silver = clean_planets_raw(df_bronze)

    print("Building Gold tables...")
    gold_tables = build_gold_tables(df_silver)

    for name, table in gold_tables.items():
        load_table_to_postgres(
            df=table,
            table_name=name,
            engine=engine,
            schema=schema,
            if_exists="replace"
        )

    print("All Gold tables loaded into Postgres.")

if __name__ == "__main__":
    main()