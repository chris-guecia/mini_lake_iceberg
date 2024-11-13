import polars as pl
import pandas as pd
import pyarrow as pa
from minio import Minio
import pyarrow.fs as pa_fs
from pyarrow import flight
import json
import time
from datetime import datetime
import uuid
from pathlib import Path


def normalize_json_to_polars(json_file_path: str) -> pl.DataFrame:
    """
    Reads a JSON file, flattens nested structures using pandas,
    and converts the result to a Polars DataFrame.
    """
    with open(json_file_path, "r") as file:
        json_data = json.load(file)

    # Flatten with pandas, using '_' as separator
    flattened_df = pd.json_normalize(json_data, sep="_")

    # Convert to polars and add needed columns
    df = pl.from_pandas(flattened_df)

    # Add batch_id and elt_created_at columns
    current_timestamp = datetime.utcnow()

    return df.with_columns([
        # Parse timestamp string with specific format
        pl.col("common_timestamp")
        .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S.%f")
        .dt.strftime("%Y-%m-%d")
        .alias("batch_id"),
        pl.lit(current_timestamp).cast(pl.Datetime).alias("elt_created_at")
    ])


def create_minio_client():
    return Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password",
        secure=False,
        region="us-east-1"
    )


def create_dremio_client():
    return flight.connect('dremio:32010')

def write_partitioned_to_minio(
    df: pl.DataFrame,
    bucket: str,
    minio_path: str,  # Changed to Path object
    partition_col: str = "batch_id",

) -> None:
    """
    Write partitioned parquet files to MinIO using Polars and PyArrow's S3FileSystem.
    """
    try:
        # Create PyArrow S3FileSystem for MinIO
        s3_fs = pa_fs.S3FileSystem(
            access_key="admin",  # MinIO root user
            secret_key="password",  # MinIO root password
            endpoint_override="minio:9000",  # MinIO endpoint
            scheme="http",
        )

        print(f"Writing partitioned parquet to s3://{bucket}/{minio_path}")

        # Construct the full path
        full_path = f"{bucket}/{minio_path}"

        df.write_parquet(
            full_path,
            use_pyarrow=True,
            pyarrow_options={"partition_cols": [partition_col],
                             "filesystem": s3_fs,
                             "existing_data_behavior": "delete_matching"},
        )

    except Exception as e:
        print(f"Error writing to MinIO: {e}")


def main():
    sample_events = "/app/data/events-sample-data.json"
    df_raw_flat = normalize_json_to_polars(json_file_path=sample_events)
    df_raw_flat.glimpse()

    print(f"making MinIO client")
    minio_client = create_minio_client

    print(f"Writing polars dataframe of {sample_events=} to parquet files in MinIO")
    write_partitioned_to_minio(
        df=df_raw_flat,
        bucket="warehouse",
        minio_path="incoming/raw/events"
    )
    print(f"finished making raw parquet")


if __name__ == "__main__":
    main()
