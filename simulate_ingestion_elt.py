import polars as pl
import pandas as pd
import pyarrow.fs as pa_fs
from pyarrow import flight
from typing import Optional
from dataclasses import dataclass
import json
from datetime import datetime
import os
import base64
import logging

# Configure logging at the start of the file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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
        logger.info(f"minio {full_path=}")

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
    Create Iceberg table and load specific batch_id with merge statement
    """
    # flight says dremio can only process 1 query at a time
    try:
        logger.info(f"Creating branch: {branch_name}")
        execute_dremio_sql(
            f"CREATE BRANCH {branch_name} FROM REF main IN nessie", client_and_token
        )
        logger.info(f"Made: branch_name={branch_name}")

        # Replace hyphens with underscores in the table name
        batch_id_clean = batch_id.replace("-", "_")
        temp_table = f"temp_fact_events_{batch_id_clean}"

        drop_in_branch = f"""
            -- Ingest Data from Parquet Files
            -- Drop then re-create version of fact_events within the branch
            -- This will help prevent duplicates if elt script is run multiple times.

            DROP TABLE IF EXISTS nessie.warehouse.{temp_table} AT BRANCH {branch_name};"""

        execute_dremio_sql(drop_in_branch, client_and_token)
        logger.info(f"Dropped fact_events in: branch_name={branch_name}")

        create_in_branch = f"""
        CREATE TABLE IF NOT EXISTS nessie.warehouse.{temp_table} (
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
        logger.info(f"Created {temp_table} in: branch_name={branch_name}")

        copy_into_branch = f"""
            COPY INTO nessie.warehouse.{temp_table} AT BRANCH {branch_name}
             FROM '@incoming/raw/events/batch_id={batch_id}'
             FILE_FORMAT 'parquet';
        """
        execute_dremio_sql(copy_into_branch, client_and_token)
        logger.info(
            f"Copied raw/events for batch_id={batch_id} into {temp_table} in: branch_name={branch_name}"
        )

        # Merge temp directly into warehouse fact table
        merge_fact_sql = f"""
        MERGE INTO nessie.warehouse.fact_events AT BRANCH {branch_name} target
        USING nessie.warehouse.{temp_table} AT BRANCH {branch_name} source
        ON target.id = source.id 
            AND target.batch_date = source.batch_date
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT VALUES (
                source.id, 
                source.user_id, 
                source.verb, 
                source.object, 
                source.product,
                source.time_stamp,
                source.batch_date,
                source.elt_created_at
            );
        """
        execute_dremio_sql(merge_fact_sql, client_and_token)
        logger.info(f"Merged batch_id={batch_id} into warehouse fact table")

        # Clean up temp table
        cleanup_sql = f"""
        DROP TABLE IF EXISTS nessie.warehouse.{temp_table} AT BRANCH {branch_name};
        """
        execute_dremio_sql(cleanup_sql, client_and_token)
        logger.info(f"Cleaned up temp table for batch_id={batch_id}")

    except Exception as e:
        logger.error(f"Error during load: {e}")
        logger.info(f"Attempting to drop branch {branch_name}")
        try:
            execute_dremio_sql(f"DROP BRANCH {branch_name} IN nessie", client_and_token)
            logger.info(f"Dropped {branch_name=}")
            # Also try to clean up temp table if it exists
            execute_dremio_sql(
                f"DROP TABLE IF EXISTS nessie.warehouse.{temp_table} AT BRANCH {branch_name};",
                client_and_token,
            )
        except Exception as e:
            logger.error(
                f"Something went wrong when trying to drop branch_name={branch_name}: {e}"
            )
        raise


@dataclass(frozen=True)
class AuditChecks:
    """Container for organizing info on what to run checks on"""

    branch_name: str
    batch_id: str
    expected_results: int


def row_count_check(audit_info: AuditChecks, client_and_token) -> bool:
    sql_check_query = f"""
    SELECT COUNT(*) AS row_count
    FROM nessie.warehouse.fact_events AT BRANCH "{audit_info.branch_name}"
    WHERE batch_date = '{audit_info.batch_id}';
    """

    df_result = execute_dremio_sql(sql_check_query, client_and_token)
    row_count = df_result["row_count"].item()
    logger.info(f"{row_count=} {audit_info.expected_results=}")
    if row_count == audit_info.expected_results:
        return True


def publish_branch(audit_info: AuditChecks, client_and_token):
    """Function that uses the row count check result"""
    try:
        if row_count_check(audit_info=audit_info, client_and_token=client_and_token):
            logger.info("Row count validation passed, proceeding with merge...")

            merge_sql = f"""
            MERGE BRANCH {audit_info.branch_name} INTO main IN nessie;;
            """

            execute_dremio_sql(merge_sql, client_and_token)
            logger.info(f"Successful merge of {audit_info.branch_name} into main")

        else:
            logger.warning("Row count validation failed, stopping process.")
            logger.warning(f"review branch_name={audit_info.branch_name} is required")

    except Exception as e:
        logger.error(f"Error in process_load: {str(e)}")
        raise


def main(files: list):
    """
    Simulate an elt pipeline writing data to s3.
    Copy data into iceberg lakehouse using Dremio and Nessie catalog.

    Args:
        files: List of JSON file paths to process
    """
    dremio_client = create_dremio_client()

    try:
        for file_path in files:
            logger.info(f"Processing file: {file_path}")

            # Normalize and write to MinIO
            df_raw_flat = normalize_json_to_polars(json_file_path=file_path)

            logger.info(f"Writing polars dataframe of file={file_path} to parquet files in MinIO")
            write_partitioned_to_minio(
                df=df_raw_flat, bucket="incoming", minio_path="raw/events"
            )
            logger.info("Finished making raw parquet")

            # Extract batch_id from filename (assuming format YYYY-MM-DD_*)
            batch_id = file_path.split('/')[-1].split('_')[0]  # Gets YYYY-MM-DD from filename

            # Create branch and load to Iceberg
            branch_name = make_branch_name()
            logger.info("Loading to Iceberg with Nessie branching...")
            setup_and_load_iceberg(
                batch_id=batch_id,
                client_and_token=dremio_client,
                branch_name=branch_name,
            )

            # Perform audit checks
            row_counts_audit = AuditChecks(
                branch_name=branch_name,
                batch_id=batch_id,
                expected_results=15
            )

            # Publish the branch
            publish_branch(audit_info=row_counts_audit, client_and_token=dremio_client)

            logger.info(f"Successfully processed file: {file_path}")

    except Exception as e:
        logger.error(f"Something went wrong {e=}")
    finally:
        dremio_client[0].close()
        logger.info("dremio_client was closed.")


if __name__ == "__main__":
    # Example usage
    files_to_process = [
        "/app/data/2023-01-01_events-sample-data.json",
        "/app/data/2023-01-02_events-sample-data.json",
    ]
    main(files=files_to_process)
