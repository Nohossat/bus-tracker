from datetime import datetime
import pandas as pd
import pytz
from pathlib import Path
import os
from google.cloud import storage

from prefect import flow, task
from prefect_gcp import GcsBucket


tz = pytz.timezone("UTC")
now = datetime.now(tz)
dt = now.replace(hour=0, minute=0, second=0, microsecond=0)


@task(log_prints=True, retries=3)
def get_timetable_from_gcs(
    current_timetable_filename: str, pref_gcs_block_name: str
) -> Path:
    """Retrieve current timetable from bucket"""

    # Prefect
    gcs_path = f"current_timetable/{current_timetable_filename}.parquet.gzip"
    gcs_block = GcsBucket.load(pref_gcs_block_name)
    # Download timetable to cwd
    gcs_block.get_directory(from_path=gcs_path)

    # Traditional
    # gcs_path = f"{current_timetable_filename}.parquet.gzip"
    # storage_client = storage.Client()
    # blobs = storage_client.list_blobs(
    #     pref_gcs_block_name, prefix="current_timetable/", delimiter="/"
    # )

    # for blob in blobs:
    #     if blob.name == f"current_timetable/{gcs_path}":
    #         blob.download_to_filename(gcs_path)

    return Path(gcs_path)


@task(log_prints=True, retries=3)
def get_live_locations_from_gcs(
    live_locations_filename: str, pref_gcs_block_name: str
) -> Path:
    """Retrieve live locations from bucket"""

    # Prefect Block
    gcs_path = f"live_location/{live_locations_filename}.parquet.gzip"
    gcs_block = GcsBucket.load(pref_gcs_block_name)
    # Download live locations to cwd
    gcs_block.get_directory(from_path=gcs_path)

    # Traditional
    # gcs_path = f"{live_locations_filename}.parquet.gzip"
    # storage_client = storage.Client()
    # blobs = storage_client.list_blobs(
    #    pref_gcs_block_name, prefix="live_location/", delimiter="/"
    # )

    # for blob in blobs:
    #     if blob.name == f"live_location/{gcs_path}":
    #        blob.download_to_filename(gcs_path)

    return Path(gcs_path)


@task()
def combine_live_trips_with_timetable(
    trips_today: pd.DataFrame, live_locations: pd.DataFrame
) -> pd.DataFrame:
    """Merge all scheduled timetable trips with live trip data"""

    compare = trips_today.merge(
        live_locations,
        left_on=["trip_id", "stop_sequence"],
        right_on=["trip_id", "current_stop"],
    )

    return compare


@task()
def calculate_late_buses(compare: pd.DataFrame) -> pd.DataFrame:
    """Calculate difference between bus scheduled time and actual live time"""

    # Resolve times that flow over to next day (e.g. 26:00 hours)
    compare.loc[:, "arrival_time_fixed"] = dt + pd.to_timedelta(compare["arrival_time"])
    compare.loc[:, "departure_time_fixed"] = dt + pd.to_timedelta(
        compare["departure_time"]
    )

    # Compare current time at stop with expected arrival time
    compare["arrival_time_fixed"] = pd.to_datetime(compare["arrival_time_fixed"])
    compare["time_diff"] = (
        compare["timestamp"] - compare["arrival_time_fixed"]
    ) / pd.Timedelta(minutes=1)

    # Remove timestamps that are not within the last 30mins
    late_buses = compare[(now - compare["timestamp"]) / pd.Timedelta(minutes=1) <= 30]

    # Get buses later than 10 minutes at specific stop and remove current_status == 1
    late_buses = late_buses[
        (late_buses["time_diff"] > 10) & (late_buses["current_status"] != 1)
    ]

    return late_buses


@task()
def load_late_buses_to_gcs(late_buses_path: Path, pref_gcs_block_name: str) -> None:
    """Upload late buses to GCS"""

    # Prefect
    gcs_block = GcsBucket.load(pref_gcs_block_name)
    gcs_block.upload_from_path(from_path=late_buses_path)

    # Traditional
    # storage_client = storage.Client()
    # gcs_block = storage_client.bucket(pref_gcs_block_name)
    # blob = gcs_block.blob(late_buses_path)
    # blob.upload_from_filename(late_buses_path)

    return None


@flow()
def compare_bus_times(
    current_timetable_filename: str = "timetable_today",
    live_locations_filename: str = "live_location-leeds",
    pref_gcs_block_name: str = "bus-tracking-data-de-nohossat",
):

    trips_today_path = get_timetable_from_gcs(
        current_timetable_filename, pref_gcs_block_name
    )
    trips_today = pd.read_parquet(trips_today_path)

    live_locations_path = get_live_locations_from_gcs(
        live_locations_filename, pref_gcs_block_name
    )
    live_locations = pd.read_parquet(live_locations_path)

    compare = combine_live_trips_with_timetable(
        wait_for=[trips_today, live_locations],  # Prefect
        trips_today=trips_today,
        live_locations=live_locations,
    )

    os.remove(trips_today_path)
    os.remove(live_locations_path)

    late_buses = calculate_late_buses(wait_for=[compare], compare=compare)  # Prefect
    late_buses.to_csv("late_buses.csv", index=False)

    load_late_buses_to_gcs(
        wait_for=[late_buses],  # Prefect
        late_buses_path="late_buses.csv",
        pref_gcs_block_name=pref_gcs_block_name,
    )

    os.remove("late_buses.csv")


if __name__ == "__main__":

    compare_bus_times()
