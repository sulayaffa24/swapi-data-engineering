import os
from google.cloud import bigquery

def load_parquet_from_gcs_to_bq(
    gcs_uri: str,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE"
):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result() # Waits for the job to complete
    print(f"Loaded {gcs_uri} into {table_id}")


def load_gold_layer_to_bigquery():
    """
    Loads all 5 gold tables from GCS -> BigQuery
    Expects files at:
        gs://{bucket}/{base}/{gold}/{table}.parquet
    """

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    bucket = os.getenv("GCS_BUCKET")
    base = os.getenv("GCS_BASE_PATH")


    gold_tables = [
        "planets",
        "planet_residents",
        "planet_films",
        "planet_terrains",
        "planet_gravity"
    ]

    for table in gold_tables:
        gcs_uri = f"gs://{bucket}/{base}/gold/{table}.parquet"
        table_id = f"{project_id}.{dataset}.{table}"

        print(f"Loading {gcs_uri} -> {table_id}")
        load_parquet_from_gcs_to_bq(gcs_uri, table_id)


if __name__ == "__main__":
    load_gold_layer_to_bigquery()