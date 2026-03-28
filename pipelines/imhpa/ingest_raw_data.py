"""
Title: ingest_raw_data.py
Author: M. G. Castrellon
Date: March 2026

Description:
Download raw gauge data from the IMHPA satellite stations API,
strip UI-only fields, wrap in an audit envelope, and save to the
bronze layer partitioned by ingestion date.
"""

# Load libraries
import json
import logging
import platform
from pathlib import Path
from imhpa import ImhpaClient
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Fields that do not provide useful information
FIELDS_TO_STRIP = ["lang_estacion", "satelitales_sensores"]

# Define Panama timezone name string
PANAMA_TZ_NAME = "America/Panama"

# Define bronze root directory
my_system = system = platform.system()

if my_system == "Windows":
    BRONZE_ROOT = Path("D:/Dropbox/Panama_Data/IMHPA/bronze")
elif my_system == "Linux":
    BRONZE_ROOT = Path("/home/gaby/Dropbox/Panama_Data/IMHPA/bronze")
else:
    raise EnvironmentError(f"Unsupported operating system: {system}")

# Load external configurations
# with open("../../local/configs.json") as json_file:
#    my_configs = json.load(json_file)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def pull_gauge_data(client: ImhpaClient, station: str, sensor: str) -> tuple[dict, str]:
    """Fetch raw JSON payload and source URL from the IMHPA API."""
    response = client.fetch_response(station, sensor)
    return response.json(), str(response.url)

def strip_fields(raw_payload: dict, fields: list) -> dict:
    """Remove UI-only fields from the payload."""
    new_payload = {}
    for k, v in raw_payload.items():
        if k not in fields:
            new_payload[k] = v
    return new_payload

def build_ingestion_timestamp(ingested_at: datetime, tz_name: str) -> dict:
    """Build a timezone-aware ingestion timestamp dictionary"""
    if ingested_at.tzinfo is None or ingested_at.utcoffset() != timedelta(0):
        raise ValueError("ingested_at must be a UTC-aware datetime.")

    local_ingested_at = ingested_at.astimezone(ZoneInfo(tz_name))

    return {
        "utc": ingested_at.isoformat(),
        "local": local_ingested_at.isoformat(),
        "timezone": tz_name,
    }

def build_envelope(payload: dict, source_url: str, ingestion_timestamp: dict) -> dict:
    """Wrap the cleaned payload in an audit envelope."""
    return {
        "source_url": source_url,
        "ingestion_timestamp": ingestion_timestamp,
        "stripped_fields": FIELDS_TO_STRIP,
        "payload": payload,
    }

def save_to_bronze(
    envelope: dict,
    station: str,
    sensor: str,
    ingested_at: datetime,
) -> Path:
    """Write the envelope to the bronze layer as a partitioned JSON file."""

    # Build partition directory: .../ingested_date=YYYY-MM-DD/
    date_str = ingested_at.strftime("%Y-%m-%d")
    partition_dir = BRONZE_ROOT / f"ingested_date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Compact timestamp for the filename: 20250101T120000
    ts_compact = ingested_at.strftime("%Y%m%dT%H%M%S")
    filename = f"data_{station}_{sensor}_{ts_compact}Z.json"

    output_path = partition_dir / filename
    output_path.write_text(
        json.dumps(envelope, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info("Saved to: %s", output_path)
    return output_path

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_ingest():
    ingested_at = datetime.now(timezone.utc)
    ingestion_timestamp = build_ingestion_timestamp(ingested_at, PANAMA_TZ_NAME)

    client = ImhpaClient()
    all_sensors = client.list_sensors(code=True)

    for sensor in all_sensors:
        all_stations = client.list_stations(sensor=sensor, ids=True)

        for station in all_stations:
            raw_payload, source_url = pull_gauge_data(client, station, sensor)
            clean_payload = strip_fields(raw_payload, FIELDS_TO_STRIP)
            envelope = build_envelope(clean_payload, source_url, ingestion_timestamp)
            save_to_bronze(envelope, station, sensor, ingested_at)

if __name__ == "__main__":
    run_ingest()