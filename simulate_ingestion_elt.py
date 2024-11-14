import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pa_fs
from pyarrow import flight
from typing import Optional
import json
import time
from datetime import datetime
import os
import base64
import logging

# Configure logging at the start of the file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


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

    # First rename the columns to drop common_ prefix
    renamed_df = df.rename(
        {
            "common_userId": "user_id",
            "common_verb": "verb",
            "common_object": "object",
            "common_product": "product",
            "common_timestamp": "time_stamp",
        }
    )

    df_final = renamed_df.with_columns(
        [
            # Parse timestamp string with specific format
            pl.col("time_stamp")
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S.%f")
            .dt.strftime("%Y-%m-%d")
            .alias("batch_id"),
            pl.col("time_stamp")
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S.%f")
            .dt.strftime("%Y-%m-%d")
            .alias("batch_date"),
            pl.lit(current_timestamp).cast(pl.Datetime).alias("elt_created_at"),
        ]
    )

    return df_final


def write_partitioned_to_minio(
        df: pl.DataFrame,
        bucket: str,
        minio_path: str,
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

        logger.info(f"Writing partitioned parquet to s3://{bucket}/{minio_path}")

        # Construct the full path
        full_path = f"{bucket}/{minio_path}"

        df.write_parquet(
            full_path,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": [partition_col],
                "filesystem": s3_fs,
                "existing_data_behavior": "delete_matching",
            },
        )

    except Exception as e:
        logger.error(f"Error writing to MinIO: {e}")


def create_dremio_client():
    """Create authenticated Dremio client using environment variables"""
    DREMIO_HOST = "dremio"
    DREMIO_PORT = 32010
    DREMIO_USER = os.getenv("DREMIO_USER")
    DREMIO_PASS = os.getenv("DREMIO_PASSWORD")

    if not DREMIO_USER or not DREMIO_PASS:
        raise ValueError("Dremio credentials not found in environment variables")

    dremio_url = f"grpc://{DREMIO_HOST}:{DREMIO_PORT}"
    logger.info(f"Connecting to Dremio at {dremio_url}")

    client = flight.connect(dremio_url)

    bearer_token = flight.FlightCallOptions(
        headers=[
            (
                b"authorization",
                f'Basic {base64.b64encode(f"{DREMIO_USER}:{DREMIO_PASS}".encode()).decode()}'.encode(),
            )
        ]
    )

    return client, bearer_token


def execute_dremio_sql(query: str, client_and_token) -> Optional[pl.DataFrame]:
    """
    Execute SQL command in Dremio and return results if it's a SELECT query

    Args:
        query: SQL query to execute
        client_and_token: Tuple of (dremio_client, bearer_token)

    Returns:
        pl.DataFrame if SELECT query, None otherwise
    """
    client, bearer_token = client_and_token
    logger.info(f"\nExecuting Dremio SQL:\n{query}")

    flight_info = client.get_flight_info(
        flight.FlightDescriptor.for_command(query), options=bearer_token
    )

    reader = client.do_get(flight_info.endpoints[0].ticket, options=bearer_token)

    # If it's a SELECT query, convert results to Polars DataFrame
    if query.strip().upper().startswith("SELECT"):
        arrow_table = reader.read_all()
        df = pl.from_arrow(arrow_table)
        return df
    else:
        reader.read_all()
        return None


def make_branch_name():
    return f"ingest_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def setup_and_load_iceberg(batch_id: str, branch_name: str, client_and_token):
    """
    Create Iceberg table and load specific batch_id with partition overwrite
    """
    # flight says dremio can only process 1 query at a time
    try:
        logger.info(f"Creating branch: {branch_name}")
        execute_dremio_sql(
            f"CREATE BRANCH {branch_name} FROM REF main IN nessie", client_and_token
        )
        logger.info(f"Made: branch_name={branch_name}")

        drop_in_branch = f"""
            -- Ingest Data from Parquet Files
            -- Drop then re-create version of fact_events within the branch
            -- This will help prevent duplicates if elt script is run multiple times.
            
            DROP TABLE IF EXISTS nessie.warehouse.fact_events AT BRANCH {branch_name};"""

        execute_dremio_sql(drop_in_branch, client_and_token)
        logger.info(f"Dropped fact_events in: branch_name={branch_name}")

        create_in_branch = f"""
        CREATE TABLE IF NOT EXISTS nessie.warehouse.fact_events (
                    id VARCHAR,
                    user_id VARCHAR,
                    verb VARCHAR,
                    object VARCHAR,
                    product VARCHAR,
                    time_stamp TIMESTAMP,
                    batch_date VARCHAR,
                    elt_created_at TIMESTAMP
                )
                AT BRANCH {branch_name}
                PARTITION BY (day(time_stamp));"""
        execute_dremio_sql(create_in_branch, client_and_token)
        logger.info(f"Created fact_events in: branch_name={branch_name}")

        copy_into_branch = f"""
            COPY INTO nessie.warehouse.fact_events AT BRANCH {branch_name}
             FROM '@incoming/raw/events/batch_id={batch_id}'
             FILE_FORMAT 'parquet';
        """
        execute_dremio_sql(copy_into_branch, client_and_token)
        logger.info(f"Copied raw/events for batch_id={batch_id} into fact_events in: branch_name={branch_name}")

    except Exception as e:
        logger.error(f"Error during load: {e}")
        logger.info(f"Attempting to drop branch {branch_name}")
        try:
            execute_dremio_sql(f"DROP BRANCH {branch_name} IN nessie", client_and_token)
        except Exception as e:
            logger.error(f"Something went wrong when trying to drop branch_name={branch_name}: {e}")
        raise


def row_count_check(branch_name: str, client_and_token, expected_result: int):
    sql_check_query = f"""
    SELECT COUNT(*) AS row_count FROM nessie.warehouse.fact_events AT BRANCH "{branch_name}";
    """

    df_result = execute_dremio_sql(sql_check_query, client_and_token)
    count = df_result["row_count"].item()
    if count == expected_result:
        return True


def publish_branch(branch_name: str, client_and_token, expected_result: int):
    """Function that uses the row count check result"""
    try:
        if row_count_check(
                branch_name, client_and_token, expected_result=expected_result
        ):
            logger.info("Row count validation passed, proceeding with merge...")

            merge_sql = f"""
            MERGE BRANCH {branch_name} INTO main IN nessie;;
            """

            execute_dremio_sql(merge_sql, client_and_token)
            logger.info(f"Successful merge of {branch_name} into main")

        else:
            logger.warning("Row count validation failed, stopping process.")
            logger.warning(f"review branch_name={branch_name} is required")

    except Exception as e:
        logger.error(f"Error in process_load: {str(e)}")
        raise


def main():
    """
    Simulate an elt pipline writing data to s3.
    Copy data into iceberg lakehouse using Dremio and Nessie catalog.
    """
    sample_events = "/app/data/events-sample-data.json"
    df_raw_flat = normalize_json_to_polars(json_file_path=sample_events)

    logger.info(f"Writing polars dataframe of sample_events={sample_events} to parquet files in MinIO")
    write_partitioned_to_minio(
        df=df_raw_flat, bucket="incoming", minio_path="raw/events"
    )
    logger.info("finished making raw parquet")

    logger.info("Starting dremio raw -> external table -> branch -> merge to main")
    dremio_client = create_dremio_client()

    try:
        branch_name = make_branch_name()
        logger.info("Loading to Iceberg with Nessie branching...")
        setup_and_load_iceberg(
            batch_id="2023-01-01",
            client_and_token=dremio_client,
            branch_name=branch_name,
        )

        publish_branch(
            branch_name=branch_name, client_and_token=dremio_client, expected_result=15
        )
    except Exception as e:
        logger.error(f"Something went wrong {e=}")
    finally:
        dremio_client[0].close()
        logger.info("dremio_client was closed.")


if __name__ == "__main__":
    main()
