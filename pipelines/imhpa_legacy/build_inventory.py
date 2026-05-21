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
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

from config.settings import LEGACY_ROOT, BRONZE_ROOT, METADATA_DIR, LOGS_DIR
DB_PATH = METADATA_DIR / "imhpa_inventory.db"


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def initialize_database(db_path: Path):
    """
    Create SQLite database with inventory and audit tables if they don't exist.
    """
    conn = sqlite3.connect(db_path)
    
    conn.execute(""""
        CREATE TABLE IF NOT EXISTS inventory (
            sensor      TEXT NOT NULL,
            station_id  TEXT NOT NULL,
            first_seen  TEXT NOT NULL,
            last_seen   TEXT NOT NULL,
            times_seen  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (sensor, station_id)
        );

        CREATE TABLE IF NOT EXISTS audit (
            ingestion_date  TEXT NOT NULL,
            sensor          TEXT NOT NULL,
            station_id      TEXT NOT NULL,
            present         INTEGER NOT NULL CHECK (present IN (0, 1)),
            PRIMARY KEY (ingestion_date, sensor, station_id),
            FOREIGN KEY (sensor, station_id)
                REFERENCES inventory (sensor, station_id)
        );
    """)
    
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def load_manifests(manifests_dir):
    file_paths = manifests_dir.glob("*.json")
    
    for file in file_path:
        

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_builder():
    initialize_database(db_path=DB_PATH)
    # Load manifests
    # Initialize inventory object
    # Populate inventory object