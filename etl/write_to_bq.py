from pathlib import Path

from dotenv import load_dotenv
import json
import os
from google.cloud import storage, bigquery

from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_file
from google.cloud.bigquery import SchemaField
from prefect.blocks.system import Secret

load_dotenv()


GCP_BUCKET = os.environ.get("GCP_BUCKET")


@task(retries=3)
def get_late_buses_from_gcs(late_buses_filename: str, pref_gcs_block_name: str) -> Path:
    """Retrieve late buses from bucket"""

    gcs_path = late_buses_filename
    # Prefect
    gcs_block = GcsBucket.load(pref_gcs_block_name)
    gcs_block.get_directory(from_path=gcs_path)

    # Traditional
    # storage_client = storage.Client()
    # gcs_block = storage_client.bucket(pref_gcs_block_name)
    # blob = gcs_block.blob(gcs_path)
    # blob.download_to_filename(gcs_path)

    return Path(gcs_path)


@flow()
def write_late_buses_bq():
    # Prefect
    secret_block = Secret.load("gcp-project-id")
    gcp_project_id = secret_block.get()
    gcp_credentials = GcpCredentials.load("bus-tracker-gcs-creds")

    pref_gcs_block_name = "bus-tracking-data-de-nohossat"
    late_buses_filename = "late_buses.csv"

    late_buses_path = get_late_buses_from_gcs(
        late_buses_filename=late_buses_filename, pref_gcs_block_name=pref_gcs_block_name
    )

    schema = [
        SchemaField("route_id", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("service_id", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("trip_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("trip_headsign", field_type="STRING", mode="REQUIRED"),
        SchemaField("block_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("shape_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("wheelchair_accessible", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("vehicle_journey_code", field_type="STRING", mode="REQUIRED"),
        SchemaField("agency_id", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_short_name", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_long_name", field_type="STRING", mode="NULLABLE"),
        SchemaField("route_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("monday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("tuesday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("wednesday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("thursday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("friday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("saturday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("sunday", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("start_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("end_date", field_type="DATE", mode="REQUIRED"),
        SchemaField("arrival_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("departure_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("stop_id", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("stop_sequence", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("stop_headsign", field_type="STRING", mode="NULLABLE"),
        SchemaField("pickup_type", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("drop_off_type", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("shape_dist_traveled", field_type="FLOAT64", mode="NULLABLE"),
        SchemaField("timepoint", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("stop_code", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("stop_name", field_type="STRING", mode="REQUIRED"),
        SchemaField("stop_lat", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("stop_long", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("wheelchair_boarding", field_type="NUMERIC", mode="NULLABLE"),
        SchemaField("location_type", field_type="STRING", mode="NULLABLE"),
        SchemaField("parent_station", field_type="STRING", mode="NULLABLE"),
        SchemaField("platform_code", field_type="STRING", mode="NULLABLE"),
        SchemaField("id", field_type="STRING", mode="REQUIRED"),
        SchemaField("route_id_live", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("start_time", field_type="TIME", mode="REQUIRED"),
        SchemaField("start_date_live", field_type="STRING", mode="REQUIRED"),
        SchemaField("latitude", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("longitude", field_type="FLOAT64", mode="REQUIRED"),
        SchemaField("current_stop", field_type="INTEGER", mode="REQUIRED"),
        SchemaField("current_status", field_type="INTEGER", mode="NULLABLE"),
        SchemaField("timestamp", field_type="STRING", mode="REQUIRED"),
        SchemaField("vehicle", field_type="STRING", mode="REQUIRED"),
        SchemaField("arrival_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("departure_time_fixed", field_type="STRING", mode="REQUIRED"),
        SchemaField("time_diff", field_type="FLOAT64", mode="REQUIRED"),
    ]

    # Prefect
    result = bigquery_load_file(
        dataset="bus_tracker",
        table="raw_late_buses",
        path=late_buses_path,
        schema=schema,
        gcp_credentials=gcp_credentials,
        project=gcp_project_id,
    )

    # Traditional
    # client = bigquery.Client()

    # if the dataset doesn't exist
    # dataset_ref = bigquery.DatasetReference.from_string(
    #     "bus_tracker", default_project="bus-tracking-421210"
    # )
    # dataset = bigquery.Dataset(dataset_ref)
    # dataset.location = "europe-west9"
    # dataset = client.create_dataset(dataset)

    # get dataset
    # dataset = client.dataset("bus_tracker")
    # table = dataset.table("raw_late_buses")

    # job_config = bigquery.LoadJobConfig(
    #    source_format=bigquery.SourceFormat.CSV, schema=schema
    # )

    # with open(late_buses_path, "rb") as source_file:
    #     job = client.load_table_from_file(source_file, table, job_config=job_config)

    # job.result()  # Waits for the job to complete.

    return result  # with Prefect
    # return None  # local


if __name__ == "__main__":
    write_late_buses_bq()
