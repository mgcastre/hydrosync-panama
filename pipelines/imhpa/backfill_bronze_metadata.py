"""
Title: backfill_bronze_metadata.py
Author: M. G. Castrellon
Date: May 2026
 
Description:
One-time migration script to backfill missing metadata fields in Bronze
JSON envelopes. Raw payloads are never modified. Files that already
contain a metadata field are skipped. A backfilled_at audit timestamp
is added to record when the migration ran.
"""
 
# Load libraries
import re
import json
import logging
import platform
from pathlib import Path
from datetime import datetime, timezone
 
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
 
my_system = platform.system()
 
if my_system == "Windows":
    BRONZE_ROOT = Path("D:/Dropbox/Panama_Data/IMHPA/bronze")
elif my_system == "Linux":
    BRONZE_ROOT = Path("/home/gaby/Dropbox/Panama_Data/IMHPA/bronze")
else:
    raise EnvironmentError(f"Unsupported operating system: {my_system}")
 
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

def extract_sensor(path: Path) -> str:
    """Parse sensor name from a Bronze JSON filename."""
    stem = path.stem
    stem = re.sub(r"^data_\d+_", "", stem)        ## drop 'data_{station_id}_'
    stem = re.sub(r"_\d{8}T\d{6}Z$", "", stem)    ## drop '_20260328T162547Z'
    return stem


def extract_station_id(path: Path) -> str:
    """Parse station_id from a Bronze JSON filename."""
    stem = path.stem
    stem = re.sub(r"^data_", "", stem)              ## drop 'data_'
    stem = re.sub(r"_.+_\d{8}T\d{6}Z$", "", stem)   ## drop '_SENSOR_timestamp'
    return stem


def backfill_file(filepath: Path, backfilled_at: str) -> tuple[str, Path]:
    """
    Backfill a single Bronze JSON envelope with missing metadata fields.
 
    Reads the envelope, checks whether metadata already exists, adds the
    metadata and backfilled_at audit fields if missing, and rewrites the
    file in place. The raw_payload is never modified.
 
    Parameters
    ----------
    filepath : Path
        Path to the Bronze JSON file to backfill.
    backfilled_at : str
        ISO 8601 UTC timestamp string for when this migration ran.
 
    Returns
    -------
    tuple[str, Path]
        A status receipt: ("processed" | "skipped" | "failed", filepath).
    """
    try:
        envelope = json.loads(filepath.read_text(encoding="utf-8"))
 
        if "station" in envelope and "sensor" in envelope:
            return "skipped", filepath
 
        station_id = extract_station_id(filepath)
        sensor = extract_sensor(filepath)
 
        envelope["station"] = station_id
        envelope["sensor"] = sensor
        envelope["backfilled_at"] = backfilled_at
 
        filepath.write_text(
            json.dumps(envelope, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
 
        logger.info("Backfilled: %s", filepath.name)
        return "processed", filepath
 
    except Exception as e:
        logger.error("Failed to backfill %s: %s", filepath.name, e)
        return "failed", filepath


def run_backfill() -> None:
    """
    Glob all Bronze JSON files and backfill missing metadata fields.
    Iterates over every JSON file under BRONZE_ROOT, calls backfill_file()
    on each, and logs a summary of processed, skipped, and failed files.
    """
    backfilled_at = datetime.now(timezone.utc).isoformat()
 
    all_files = list(BRONZE_ROOT.rglob("data*.json"))
    logger.info("Found %d JSON files to process.", len(all_files))
 
    results = {"processed": [], "skipped": [], "failed": []}
 
    for filepath in all_files:
        status, path = backfill_file(filepath, backfilled_at)
        results[status].append(path)
 
    logger.info(
        "Backfill complete — processed: %d, skipped: %d, failed: %d",
        len(results["processed"]),
        len(results["skipped"]),
        len(results["failed"]),
    )
 
    if results["failed"]:
        logger.warning("The following files could not be backfilled:")
        for path in results["failed"]:
            logger.warning("  %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
 
if __name__ == "__main__":
    run_backfill()
