"""
Title: ingest.py
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
from pathlib import Path
from imhpa import ImhpaClient
from datetime import datetime, timezone
from pipelines.imhpa.audit import Manifest
from pipelines.utils.timestamps import to_local_time
from pipelines.utils.timestamps import PANAMA_TZ_NAME
from core.storage_backend import StorageBackend

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Define fields that do not provide useful information
FIELDS_TO_STRIP = ["lang_estacion", "satelitales_sensores"]

# Setup logging
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
    """Build a timestamp dictionary with both UTC and local times."""
    local_dt = to_local_time(ingested_at, tz_name)
    return {
        "utc": ingested_at.isoformat(),
        "local": local_dt.isoformat(),
        "timezone": tz_name,
    }

def build_envelope(payload: dict, source_url: str, 
                   station: str, sensor: str,
                   ingestion_timestamp: dict) -> dict:
    """Wrap the cleaned payload in an audit envelope."""
    return {
        "station": station,
        "sensor": sensor,
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
    backend: StorageBackend
) -> None:
    """Save the envelope to the bronze layer using the provided storage backend."""

    # Compact timestamp for the filename: 20250101T120000
    ts_compact = ingested_at.strftime("%Y%m%dT%H%M%S")
    file_name = f"data_{station}_{sensor}_{ts_compact}Z.json"

    # Build relative path: ingested_date=YYYY-MM-DD/file_name.json
    date_str = ingested_at.strftime("%Y-%m-%d")
    partition_dir = Path(f"ingested_date={date_str}")
    relative_path = partition_dir / file_name

    # Prepare content
    json_string = json.dumps(envelope, ensure_ascii=False, indent=2)
    content = json_string.encode("utf-8")

    # Pass to backend storage handler and log
    output_path = backend.save(content, relative_path)
    logger.info("Saved to: %s", output_path)

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_ingest(backend: StorageBackend):
    ingested_at = datetime.now(timezone.utc)
    ingestion_timestamp = build_ingestion_timestamp(ingested_at, PANAMA_TZ_NAME)

    client = ImhpaClient()
    all_sensors = client.list_sensors(code=True)

    saved = 0
    failed = 0

    manifest = Manifest(ingested_at)

    for sensor in all_sensors:
        all_stations = client.list_stations(sensor=sensor, ids=True)

        for station in all_stations:
            try:
                raw_payload, source_url = pull_gauge_data(client, station, sensor)
                clean_payload = strip_fields(raw_payload, FIELDS_TO_STRIP)
                envelope = build_envelope(
                    payload=clean_payload, 
                    source_url=source_url,
                    station=station, sensor=sensor,
                    ingestion_timestamp=ingestion_timestamp,
                    )
                save_to_bronze(envelope, station, sensor, ingested_at, backend)
                manifest.add_pair(sensor, station)
                saved += 1
            
            except Exception as e:
                 failed += 1
                 logger.error("Failed (%s, %s): %s", sensor, station, e)
    
    manifest.write(backend)
    logger.info("Ingestion complete — saved: %d, failed: %d", saved, failed)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    # Load backend and settings
    from core.storage_backend import LocalStorage
    from config.settings import BRONZE_ROOT

    # Configure backend
    local_storage = LocalStorage(root=Path(BRONZE_ROOT))
    
    # Run ingest
    run_ingest(backend=local_storage)