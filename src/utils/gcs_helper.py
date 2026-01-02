import io
import pandas as pd
from google.cloud import storage
import os

def upload_df_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    print(f"Uploaded to gs://{bucket_name}/{blob_path}")