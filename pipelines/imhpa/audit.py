"""
Title: audit.py
Author: M. G. Castrellon
Date: May 2026

Description:
"""

# Load libraries
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# Define classes and functions

class Manifest:
    def __init__(self, ingested_at: datetime, manifests_dir: Path):
        self.ingested_at = ingested_at
        self.manifests_dir = manifests_dir
        self.pairs = []
    
    @property
    def ingestion_date(self) -> str:
        return self.ingested_at.strftime("%Y-%m-%d")
    
    @property
    def as_dict(self) -> dict:
        return {
            "ingestion_date": self.ingestion_date,
            "number_of_pairs": len(self.pairs),
            "sensor_station_pairs": self.pairs,
        }
    
    def add_pair(self, sensor: str, station: str):
        self.pairs.append((sensor, station))
    
    def write(self):
        self.manifests_dir.mkdir(parents=True, exist_ok=True)

        file_name = f"{self.ingestion_date}_ingestion_manifest.json"
        output_path = self.manifests_dir / file_name

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.as_dict, f, indent=4)


class Inventory:
    def __init__(self, db_path: Path):
        self.db_path = db_path
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        return self
    
    def __exit__(self, *args):
        self.conn.close()
    
    def _initialize(self):
        self.conn.executescript("""
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

    def upsert_inventory(self, manifest: dict):
        pass
        # I am going to pretend the table is a csv, because I don't know
        # how to work with sql tables just yet.
        
        # 1) The first thing that I have to do is to compare what I have in the dict
        #    with what I have in the table.

        

    def upsert_audit(self, manifest: dict):
        pass

    def update(self, manifest: dict):
        self.upsert_inventory(manifest)
        self.upsert_audit(manifest)