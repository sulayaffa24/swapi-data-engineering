import re
import os
from src.utils.gcs_helper import upload_df_to_gcs
import numpy as np
import pandas as pd

from src.extract.swapi_client import get_normalized_planets

def clean_planets_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Silver-Layer:
    - Standardize text
    - add surrogate key (planet_id)
    - create numeric versions for numeric columns
    - create terrain_list and gravity_list
    - create gravity_clean and gravity_numeric
    """

    df = df.copy()

    text_cols = [
        "rotation_period", "orbital_period", "diameter", "climate",
        "gravity", "terrain", "surface_water", "population"
    ]

    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    df = df.replace({
        "N/A": "unknown", 
        "n/a": "unknown", 
        "None": "unknown", 
        "": "unknown"
        })

    # Create Surrogate key (planet_id)
    planets_df = df[["name"]].drop_duplicates().reset_index(drop=True)
    planets_df["planet_id"] = planets_df.index + 1 # 1, 2, 3

    # Merge it to the main dataframe
    df = df.merge(planets_df, on="name", how="left")

    # Numeric versions of numeric-like columns
    num_cols = [
        "rotation_period", "orbital_period", "diameter",
        "surface_water", "population"
    ]

    for col in num_cols:
        df[f"{col}_num"] = pd.to_numeric(
            df[col].where(df[col] != "unknown"),
            errors="coerce"
        )

    # Gravity cleanup - gravity_list, gravity_clean
    df["gravity_list"] = df["gravity"].str.split(r",\s*")

    def normalize_gravity_list(g_list):
        cleaned = []
        for item in g_list:
            if item == "unknown":
                cleaned.append("unknown")
            elif re.fullmatch(r"\d+(\.\d+)?", item):
                cleaned.append(f"{item} standard")
            else:
                cleaned.append(item)
        return cleaned

    df["gravity_list"] = df["gravity_list"].apply(normalize_gravity_list)

    def extract_numeric(g):
        match = re.search(r"(\d+(\.\d+)?)", g)
        return float(match.group(1)) if match else np.nan

    df["gravity_clean"] = df["gravity_list"].apply(lambda x: x[0] if x else "unknown")
    df["gravity_numeric"] = df["gravity_clean"].apply(extract_numeric)

    # Terrain -> terrain_list
    df["terrain_list"] = df["terrain"].str.split(r",\s*")

    return df


def build_gold_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Gold-Layer tables:
    - planets
    - planet_residents
    - planet_films
    - planet_terrains
    - planet_gravity
    """
    df = df.copy()

    # planets table
    planets = df[[
        "planet_id", "name", "rotation_period",
        "rotation_period_num", "orbital_period",
        "orbital_period_num", "diameter", "diameter_num",
        "climate", "gravity_clean", "gravity_numeric",
        "terrain", "surface_water", "surface_water_num",
        "population", "population_num", "created", "edited"
    ]].drop_duplicates(subset=["planet_id"]).reset_index(drop=True)

    #planet_residents
    planet_residents = (
        df[["planet_id", "residents"]]
        .explode("residents")
        .dropna(subset=["residents"])
        .rename(columns={"residents": "resident_name"})
        .reset_index(drop=True)
    )

    # planet_films
    planet_films = (
        df[["planet_id", "films"]]
        .explode("films")
        .dropna(subset=["films"])
        .rename(columns={"films": "film_title"})
        .reset_index(drop=True)
    )

    # planet_terrains
    planet_terrains = (
        df[["planet_id", "terrain_list"]]
        .explode("terrain_list")
        .dropna(subset=["terrain_list"])
        .rename(columns={"terrain_list": "terrain"})
        .reset_index(drop=True)
    )

    # planet_gravity
    planet_gravity = (
        df[["planet_id", "gravity_list"]]
        .explode("gravity_list")
        .dropna(subset=["gravity_list"])
        .rename(columns={"gravity_list": "gravity_value"})
        .reset_index(drop=True)
    )

    return {
        "planets": planets,
        "planet_residents": planet_residents,
        "planet_films": planet_films,
        "planet_terrains": planet_terrains,
        "planet_gravity": planet_gravity
    }

if __name__ == "__main__":
    # Bronze: extract
    df_bronze = get_normalized_planets()

    # silver: clean
    df_silver = clean_planets_raw(df_bronze)

    # Gold: modeled tables
    gold_tables = build_gold_tables(df_silver)

    # Googe Cloud Storage
    bucket = os.getenv("GCS_BUCKET")
    base = os.getenv("GCS_BASE_PATH", "swapi")

    # Bronze
    upload_df_to_gcs(df_bronze, bucket, f"{base}/bronze/planets_raw.parquet")

    # Silver
    upload_df_to_gcs(df_silver, bucket, f"{base}/silver/planets_clean.parquet")

    # Gold
    for name, table in gold_tables.items():
        upload_df_to_gcs(table, bucket, f"{base}/gold/{name}.parquet")