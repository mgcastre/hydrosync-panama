"""
Title: backfill_manifests.py
Author: M. G. Castrellon
Date: May 2026
 
Description:
Retroactively build ingestion manifests for all existing Bronze and Legacy
(raw) partitions. This is a one-time backfill utility. Going forward, the 
ingestion pipeline will produce manifests at ingestion time.
 
A manifest is a JSON file recording which (sensor, station) pairs were
present in a given ingestion batch. One manifest is written per ingestion
date, stored in a dedicated _manifests/ directory at each layer root.
 
Output paths:
    <bronze_root>/_manifests/YYYY-MM-DD_ingestion_manifest.json
    <legacy_root>/_manifests/YYYY-MM-DD_ingestion_manifest.json
"""

# Load libraries
import json
import logging
import platform
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
 
my_system = platform.system()
 
if my_system == "Windows":
    DATA_ROOT = Path("D:/Dropbox/Panama_Data/IMHPA")
elif my_system == "Linux":
    DATA_ROOT = Path("/home/gaby/Dropbox/Panama_Data/IMHPA")
else:
    raise EnvironmentError(f"Unsupported operating system: {my_system}")

BRONZE_ROOT = DATA_ROOT / "bronze"
LEGACY_ROOT = DATA_ROOT / "raw"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
"""
Note:
Legacy Parquet files store sensor names as full display strings (in Spanish).
This mapping translates them to the canonical sensor codes used everywhere
else in HydroSync Panama.
"""

SENSOR_MAPPING = {
    "Monóxido de Carbono (CO)": "CO",
    "Viento - Dirección a 10m": "DIR_VIENTO10-DIR_VIENTO",
    "Dirección del Viento 2m": "DIR_VIENTO2",
    "Horas de Brillo Solar": "HORA_SOL",
    "Humedad Relativa Promedio": "HR_PROM",
    "Lluvia": "LLUVIA",
    "Nivel": "NIVEL",
    "Monóxido de Nitrógeno (NO)": "NO",
    "Dióxido de Nitrógeno (NO&sup2;)": "NO2",
    "Ozono (O&sup3;)": "O3",
    "Presión Barométrica": "P_BAROM",
    "Punto de Rocío": "P_ROCIO",
    "Radiación Solar": "RAD_SOLAR",
    "Velocidad Máxima del Viento": "RAFAGA",
    "Relación de Mezcla": "R_MEZCLA",
    "Temperatura Promedio": "TEMP_PROM",
    "Velocidad del Viento a 10m": "VEL_VIENTO-VEL_VIENTO10",
    "Velocidad del Viento a 2m": "VEL_VIENTO2",
}

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

def read_pairs_from_bronze_folder(partition_dir: Path) -> list[tuple[str, str]]:
    """
    Read all Bronze envelope files in one partition folder and return the
    (sensor, station) pairs found. Each file is expected to be a JSON envelope 
    with top-level 'sensor' and 'station' keys.
 
    Args:
        partition_dir: Path to a single ingested_date=YYYY-MM-DD folder.
 
    Returns:
        List of (sensor, station) tuples. Empty list if no data files found.
    """
    data_files = list(partition_dir.glob("data*.json"))
 
    if not data_files:
        logger.warning(f"No data files in {partition_dir.name}, skipping.")
        return []
 
    pairs = []
    for file_path in data_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                envelope = json.load(f)
            pairs.append((envelope["sensor"], envelope["station"]))
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Could not read {file_path.name}: {e}")
 
    return pairs
 
 
def read_pairs_from_legacy_parquets(
parquet_files: list[Path],
) -> dict[str, list[tuple[str, str]]]:
    """
    Load all legacy Parquet files and return (sensor, station) pairs grouped
    by ingestion date.Legacy files store sensor names as full Spanish display strings. 
    This function applies SENSOR_MAPPING to translate them to canonical codes.
    Unmapped sensor names are logged as warnings and excluded.
 
    Args:
        parquet_files: List of Parquet file paths to load.
 
    Returns:
        Dict mapping ingestion date strings (YYYY-MM-DD) to lists of
        (sensor, station) tuples. Returns an empty dict if no files given.
    """
    if not parquet_files:
        logger.warning("No legacy Parquet files found.")
        return {}
 
    logger.info(f"Loading {len(parquet_files)} legacy Parquet file(s).")
 
    df = pd.concat(
        [pd.read_parquet(f) for f in parquet_files],
        ignore_index=True,
    )
    df = df[["sensor", "station_id", "ingest_timestamp"]].drop_duplicates()

    # Identify unmapped sensor names before applying the mapping
    unmapped_mask = ~df["sensor"].isin(SENSOR_MAPPING.keys())
    unmapped_names = sorted(df.loc[unmapped_mask, "sensor"].unique())
 
    if unmapped_names:
        logger.warning(
            f"{unmapped_mask.sum()} row(s) had unmapped sensor names and will be excluded."
        )
        logger.warning("Unmapped sensor names:")
        for name in unmapped_names:
            logger.warning(f"  - '{name}'")

    # Apply mapping and drop rows whose sensor name was not recognised.
    df["sensor"] = df["sensor"].map(SENSOR_MAPPING)
    df = df.dropna(subset=["sensor"])

    # Derive ingestion date from timestamp
    df["ingest_timestamp"] = pd.to_datetime(df["ingest_timestamp"])
    df["ingestion_date"] = df["ingest_timestamp"].dt.date.astype(str)
    df.drop(columns=["ingest_timestamp"], inplace=True)
 
    # Group pairs by ingestion date
    pairs_by_date: dict[str, list[tuple[str, str]]] = {}
    for date, group in df.groupby("ingestion_date"):
        pairs_by_date[date] = list(
            group[["sensor", "station_id"]].itertuples(index=False, name=None)
        )
 
    logger.info(f"Found {len(pairs_by_date)} ingestion date(s) in legacy files.")
    return pairs_by_date
 

def build_manifest(ingestion_date: str, pairs: list[tuple[str, str]]) -> dict:
    """
    Build a manifest dict describing one ingestion batch.
 
    Args:
        ingestion_date: The ingestion date as a YYYY-MM-DD string.
        pairs: List of (sensor, station) tuples found in this batch.
 
    Returns:
        A manifest dict ready to be serialised to JSON.
    """
    return {
        "ingestion_date": ingestion_date,
        "number_of_pairs": len(pairs),
        "sensor_station_pairs": pairs,
        "backfilled_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
 

def write_manifest(manifest: dict, manifests_dir: Path) -> Path:
    """
    Write a manifest dict to the _manifests directory.
 
    Skips writing if a manifest for this date already exists (idempotent).
 
    Args:
        manifest: The manifest dict produced by build_manifest().
        manifests_dir: Path to the _manifests output directory.
 
    Returns:
        Path to the written (or skipped) manifest file.
    """
    ingestion_date = manifest["ingestion_date"]
    file_name = f"{ingestion_date}_ingestion_manifest.json"
    output_path = manifests_dir / file_name
 
    if output_path.exists():
        logger.info(f"Manifest already exists, skipping: {file_name}")
        return output_path
 
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)
 
    logger.info(f"Written: {file_name}  ({manifest['number_of_pairs']} pairs)")
    return output_path

# ---------------------------------------------------------------------------
# Orchestrators
# ---------------------------------------------------------------------------
 
def backfill_bronze(bronze_root: Path) -> dict:
    """
    Iterate over all Bronze partition folders and write one manifest per folder.
 
    Only folders matching ingested_date=YYYY-MM-DD are processed. Folders
    with no data files are skipped with a warning.
 
    Args:
        bronze_root: Root path of the Bronze layer.
 
    Returns:
        Summary dict with counts: processed, skipped, failed.
    """
    manifests_dir = bronze_root / "_manifests"
    manifests_dir.mkdir(exist_ok=True)
 
    partition_dirs = sorted(bronze_root.glob("ingested_date=*"))
 
    if not partition_dirs:
        logger.warning(f"No partition folders found under {bronze_root}")
        return {"processed": 0, "skipped": 0, "failed": 0}
 
    logger.info(f"Bronze: {len(partition_dirs)} partition(s) found.")
    summary = {"processed": 0, "skipped": 0, "failed": 0}
 
    for partition_dir in partition_dirs:
        ingestion_date = partition_dir.name.split("=")[-1]
 
        pairs = read_pairs_from_bronze_folder(partition_dir)
 
        if not pairs:
            summary["skipped"] += 1
            continue
 
        try:
            manifest = build_manifest(ingestion_date, pairs)
            write_manifest(manifest, manifests_dir)
            summary["processed"] += 1
        except Exception as e:
            logger.error(f"Failed to write manifest for {ingestion_date}: {e}")
            summary["failed"] += 1
 
    return summary


def backfill_legacy(legacy_root: Path) -> dict:
    """
    Load all legacy Parquet files under raw/ and write one manifest per
    ingestion date found.
 
    Args:
        legacy_root: Root path of the legacy raw folder.
 
    Returns:
        Summary dict with counts: processed, skipped, failed.
    """
    manifests_dir = legacy_root / "_manifests"
    manifests_dir.mkdir(exist_ok=True)
 
    parquet_files = list(legacy_root.rglob("**/*.parquet"))
 
    if not parquet_files:
        logger.warning(f"No Parquet files found under {legacy_root}")
        return {"processed": 0, "skipped": 0, "failed": 0}
 
    pairs_by_date = read_pairs_from_legacy_parquets(parquet_files)
 
    summary = {"processed": 0, "skipped": 0, "failed": 0}
 
    for ingestion_date, pairs in sorted(pairs_by_date.items()):
        if not pairs:
            logger.warning(f"No pairs for date {ingestion_date}, skipping.")
            summary["skipped"] += 1
            continue
 
        try:
            manifest = build_manifest(ingestion_date, pairs)
            write_manifest(manifest, manifests_dir)
            summary["processed"] += 1
        except Exception as e:
            logger.error(f"Failed to write manifest for {ingestion_date}: {e}")
            summary["failed"] += 1
 
    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_backfill() -> None:
    logger.info("=" * 60)
    logger.info("Starting manifest backfill.")
    logger.info(f"  Bronze root : {BRONZE_ROOT}")
    logger.info(f"  Legacy root : {LEGACY_ROOT}")
    logger.info("=" * 60)
 
    logger.info("--- Bronze layer ---")
    bronze_summary = backfill_bronze(BRONZE_ROOT)
    logger.info(f"  Processed : {bronze_summary['processed']}")
    logger.info(f"  Skipped   : {bronze_summary['skipped']}")
    logger.info(f"  Failed    : {bronze_summary['failed']}")
 
    logger.info("--- Legacy layer ---")
    legacy_summary = backfill_legacy(LEGACY_ROOT)
    logger.info(f"  Processed : {legacy_summary['processed']}")
    logger.info(f"  Skipped   : {legacy_summary['skipped']}")
    logger.info(f"  Failed    : {legacy_summary['failed']}")
 
    logger.info("Backfill complete.")


if __name__ == "__main__":
    run_backfill()

