"""
Title: ingest_raw_data.py
Author: M. G. Castrellon
Date: February 2026

Description:
Download raw gauge data from the IMHPA satellite stations API,
strip UI-only fields, wrap in an audit envelope, and save to the
bronze layer partitioned by ingestion date.
"""

import json
import imhpa
import logging
import platform
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Fields that are UI rendering artefacts — not data
FIELDS_TO_STRIP = ["lang_estacion", "satelitales_sensores"]

# Timezone for Panama
PANAMA_TZ = timezone(timedelta(hours=-5))

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

def strip_fields(payload: dict, fields: list) -> dict:
    """Remove UI-only fields from the payload."""
    return {k: v for k, v in payload.items() if k not in fields}


def build_envelope(raw_payload: dict, ingested_at: str) -> dict:
    """Wrap the cleaned payload in an audit envelope."""
    return {
        "ingested_at": ingested_at,
        "stripped_fields": FIELDS_TO_STRIP,
        "raw_payload": raw_payload,
    }


def save_to_bronze(envelope: dict, station: str, sensor: str, ingested_at: str) -> Path:
    """Write the envelope to the bronze layer partition."""
    # Partition folder by ingestion date
    date_str = ingested_at[:10]  # YYYY-MM-DD
    partition_dir = BRONZE_ROOT / f"ingested_date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Filename carries full timestamp + station/sensor for traceability
    ts_compact = ingested_at.replace(":", "").replace("-", "")[:15]
    filename = f"imhpa_raw_{station}_{sensor}_{ts_compact}Z.json"
    output_path = partition_dir / filename

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved to: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_ingest():
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for req in REQUESTS:
        station = req["estacion"]
        sensor = req["sensor"]

        raw_payload, source_url = fetch_gauge_data(station, sensor)
        clean_payload = strip_fields(raw_payload, FIELDS_TO_STRIP)
        envelope = build_envelope(clean_payload, source_url, ingested_at)
        save_to_bronze(envelope, station, sensor, ingested_at)


if __name__ == "__main__":
    run_ingest()