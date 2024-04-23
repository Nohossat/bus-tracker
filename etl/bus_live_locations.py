from datetime import datetime
import pandas as pd
import pytz
import os
import requests
from google.transit.gtfs_realtime_pb2 import FeedMessage
from http import HTTPStatus
from dotenv import load_dotenv

from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.blocks.system import Secret

from google.cloud import storage


# For local env variable
# load_dotenv()
# bods_api_key = os.environ.get("BODS_API")

# Prefect Block
secret_block = Secret.load("bods-api-key")
bods_api_key = secret_block.get()

# data format
"""
id: "3686484612494717199"
vehicle {
  trip {
    trip_id: "VJeb0333173d9592d25f35f260e81f9627efb7cc2a"
    route_id: "6639"
    start_time: "22:35:00"
    start_date: "20240422"
    schedule_relationship: SCHEDULED
  }
  vehicle {
    id: "YY18TKJ"
    label: "YY18TKJ"
  }
  position {
    latitude: 51.5661163
    longitude: 0.236009
    bearing: 45
  }
  current_stop_sequence: 27
  current_status: IN_TRANSIT_TO
  timestamp: 1713826491
}
"""


@task(log_prints=True, retries=3)
def get_live_gtfs(
    min_lat: float, max_lat: float, min_long: float, max_long: float, filename: str
) -> None:
    """Get live bus locations from Open Bus Data GTFS feed for area specified by bounding box coordinates"""

    url = f"https://data.bus-data.dft.gov.uk/api/v1/gtfsrtdatafeed/?boundingBox={min_lat},{max_lat},{min_long},{max_long}&api_key={bods_api_key}"

    response = requests.get(url, bods_api_key)

    if response.status_code == HTTPStatus.OK:
        message = FeedMessage()
        message.ParseFromString(response.content)

    trips = []
    for t in message.entity:
        trips.append(t)

    # transform GFTS to tabular - list comprehension
    rows = [
        {
            "id": t.id,
            "trip_id": t.vehicle.trip.trip_id,
            "route_id": t.vehicle.trip.route_id,
            "start_time": t.vehicle.trip.start_time,
            "start_date": t.vehicle.trip.start_date,
            "latitude": t.vehicle.position.latitude,
            "longitude": t.vehicle.position.longitude,
            "current_stop": t.vehicle.current_stop_sequence,
            "current_status": t.vehicle.current_status,
            "timestamp": pd.to_datetime(t.vehicle.timestamp, utc=True, unit="s"),
            "vehicle": t.vehicle.vehicle.id,
        }
        for t in trips
    ]

    df = pd.DataFrame(rows).drop_duplicates()

    # Remove data not published on current date
    tz = pytz.timezone("UTC")
    today = datetime.now(tz).date()

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")

    df = df[df["timestamp"].dt.date == today]

    # Rename fields
    df.rename(
        {"start_date": "start_date_live", "route_id": "route_id_live"},
        axis=1,
        inplace=True,
    )

    df.to_parquet(f"{filename}.parquet.gzip", compression="gzip")

    return None


@task()
def load_live_locations_to_gcs(
    pref_gcs_block_name: str, from_path: str, to_path: str
) -> None:
    """Load the live bus locations to Google Bucket"""

    # Prefect
    gcs_block = GcsBucket.load(pref_gcs_block_name)  # - Prefect Block
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path)

    # Traditional
    # storage_client = storage.Client()
    # gcs_block = storage_client.bucket(pref_gcs_block_name)
    # blob = gcs_block.blob(to_path)
    # blob.upload_from_filename(from_path)

    os.remove(from_path)
    return None


@flow()
def get_live_bus_locations(
    area_coords: dict = {
        "min_lat": 53.725,
        "max_lat": 53.938,
        "min_long": -1.712,
        "max_long": -1.296,
    },
    pref_gcs_block_name: str = "bus-tracking-data-de-nohossat",
    live_locations_filename: str = "live_location-leeds",
):

    get_live_gtfs(
        area_coords.get("min_lat"),
        area_coords.get("max_lat"),
        area_coords.get("min_long"),
        area_coords.get("max_long"),
        filename=live_locations_filename,
    )

    load_live_locations_to_gcs(
        wait_for=[get_live_gtfs],  # prefect
        pref_gcs_block_name=pref_gcs_block_name,
        from_path=f"{live_locations_filename}.parquet.gzip",
        to_path=f"live_location/{live_locations_filename}.parquet.gzip",
    )


if __name__ == "__main__":

    get_live_bus_locations()
