"""
Title: build_inventory.py
Author: M. G. Castrellon
Date: May 2026
 
Description:
"""

# Load libraries
import json
import logging
import sqlite3
import platform
from pathlib import Path

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

LEGACY_ROOT = DATA_ROOT / "raw"
BRONZE_ROOT = DATA_ROOT / "bronze"
METADATA_DIR = DATA_ROOT / "_metadata"
LOGS_DIR = DATA_ROOT / "_logs"

DB_PATH = METADATA_DIR / "imhpa_inventory.db"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def initialize_database(db_path: Path):
    """
    Create SQLite database with inventory and audit tables if they don't exist.
    """
    conn = sqlite3.connect(db_path)
    
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory (
            sensor TEXT NOT NULL,
            station_id TEXT NOT NULL,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            times_seen INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (sensor, station_id)
        );
        """
    )
    
    conn.commit()
    conn.close()