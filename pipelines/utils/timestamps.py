"""
timestamps.py
"""

# Load libraries
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

# Define constants ========================================================
PANAMA_TZ_NAME = "America/Panama"

# Define utility functions ================================================

def to_local_time(utc_dt: datetime, tz_name: str) -> datetime:
    """Convert a UTC-aware datetime to a local timezone-aware datetime."""
    if utc_dt.tzinfo is None or utc_dt.utcoffset() != timedelta(0):
        raise ValueError("utc_dt must be a UTC-aware datetime.")
    return utc_dt.astimezone(ZoneInfo(tz_name))