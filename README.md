# Event Data Pipeline with Dremio, Nessie, and Apache Iceberg

This project uses a python script to simulate an ingestion an ELT (Extract, Load, Transform) pipeline that processes JSON event data through MinIO (S3-compatible storage), Dremio, and Apache Iceberg with Nessie version control.
Credit to Dremio 
https://www.dremio.com/blog/intro-to-dremio-nessie-and-apache-iceberg-on-your-laptop/
## Architecture Overview

The pipeline consists of the following components:
- **MinIO**: S3-compatible object storage for raw data
- **Dremio**: SQL query engine and data lake engine
- **Apache Iceberg**: Table format for large analytic datasets
- **Nessie**: Git-like version control for your data lake

## Prerequisites

- Docker and Docker Compose
- Python 3.8+

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com/chris-guecia/mini_lake_iceberg.git
   ```

2. **Follow this guide provided by Dremio**  
   https://www.dremio.com/blog/intro-to-dremio-nessie-and-apache-iceberg-on-your-laptop/
   
   At the part when making the dremio account with username and password  
   set them to the following.
   Access Dremio UI at `http://localhost:9047`
   the name and email can be anything for example admin@admin.com
   make an account with these creds: These are used in the simulate_ingestion_elt.py script
      ```
      Username: admin
      Password: dremio_password123
      ```
   Everything else in that guide is the same.
```
docker-compose up dremio
docker-compose up minio
docker-compose up nessie

-- To run the simulate_ingestion_elt.py script 
docker-compose up --build simulate-ingestion-elt 
docker exec simulate-ingestion-elt python simulate_ingestion_elt.py 
```
3. **Configure Dremio source for incoming raw events**

   Configure MinIO source in Dremio:
      - Click "Add Source" and select "S3"
      - Configure the following settings:

      **General Settings:**
      ```
      Name: incoming
      Credentials: AWS Access Key
      Access Key: admin
      Secret Key: password
      Encrypt Connection: false
      ```

      **Advanced Options:**
      ```
      Enable Compatibility Mode: true
      Root Path: /incoming
      ```

      **Connection Properties:**
      ```
      fs.s3a.path.style.access = true
      fs.s3a.endpoint = minio:9000
      ```


 **Initialize Data Warehouse Schema**
   1. In Dremio UI, navigate to the SQL Editor
   2. Open and paste the sql: `sql/DDL-sample-dw.sql` into an editor and hit run.
   3. This will create the necessary tables and schema for the data warehouse

## Docker Container Details

The project includes a Python container for simulating data ingestion:

```yaml
simulate-ingestion-elt:
  build:
    context: .
    dockerfile: Dockerfile
  container_name: simulate-ingestion-elt
  networks:
    - iceberg
  volumes:
    - ./data:/app/data
  environment:
    - DREMIO_USER=admin
    - DREMIO_PASSWORD=dremio_password123
  depends_on:
    - minio
    - dremio
    - nessie
```

This container:
- Mounts the local `./data` directory to `/app/data` in the container
- Uses predefined Dremio credentials
- Runs after MinIO, Dremio, and Nessie services are started
- Connects to the `iceberg` network for communication with other services

## Running the Pipeline

The pipeline performs the following steps:
The script is idempotent meaning re-running won't make duplicates in the warehouse  
The script follows W.A.P. Write -> Audit -> Publish using Nessie catalog with Apache Iceberg with  
Dremio as the compute /sql engine 
1. Reads JSON event data
2. Flattens and normalizes the data using Polars
3. Writes partitioned Parquet files to MinIO
4. Creates a new Nessie branch
5. Loads data into an Iceberg table
6. Performs validation
7. Merges changes to main branch

To run the pipeline:
```bash
docker exec simulate-ingestion-elt python app/main.py
```
In Dremio You should see this in sources in http://localhost:9047/  
![dremio_datasets](https://github.com/user-attachments/assets/6bae0e28-99ea-4949-9ed2-bc4a15e5c626)


The warehouse should look like this 
![dremio_6](https://github.com/user-attachments/assets/0e3998a2-4d30-49ce-969c-ff80e935bcb3)


Here shows different Nessie Branches in Dremio UI  
![dremio_branches](https://github.com/user-attachments/assets/2a1c856b-86f3-499a-8297-be9c187927f1)


And MinIO should look like this (object counts in warehouse will differ) http://localhost:9001/browser  
![minIO_1](https://github.com/user-attachments/assets/1bd9bf3d-f04b-461b-88c0-4a2e904de51a)


## Analytical SQL query results
Due to the sample JSON being only 1 days worth of events 2023-01-01, queries looking back a week from CURRENT_TIMESTAMP   
won't return results I made some changes to the queries to show results based on the sample event timestamps

![dremio_sql_result_4](https://github.com/user-attachments/assets/b22de3ff-f8dc-4103-83b3-130f965852d2)

![dremio_sql_result_3](https://github.com/user-attachments/assets/e3c0ab17-8a56-4099-a891-1b184a5030e1)

ERD
![amplify_erd](https://github.com/user-attachments/assets/58c19d5c-daff-4c8f-9944-8885a4b2068d)


Additional tables and schema details can be found in `sql/DDL-sample-dw.sql`.

## Key Features

- **Data Versioning**: Uses Nessie branches for safe data loading and validation
- **Partitioned Storage**: Data is partitioned by batch_id for efficient querying
- **Data Quality Checks**: Includes row count validation before merging changes
- **Idempotent Operations**: Supports multiple runs without duplicating data
- **Error Handling**: Includes branch cleanup on failure

## Next Steps
- Add transformations to add dimension surrogate_keys to the fact_events table for better joins 
