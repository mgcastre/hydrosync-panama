"""
settings.py
"""

# Load libraries
import platform
from pathlib import Path

# Define data root directory (Dropbox)
my_system = system = platform.system()

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