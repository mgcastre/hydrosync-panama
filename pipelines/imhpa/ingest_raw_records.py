"""
Script to download raw data from IMHPA server
M. G. Castrellon | February 2026
"""

# Load libraries
import os
import imhpa
import logging
import platform
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Identify OS running
system = platform.system()

# Initialize the client
client = imhpa.ImhpaClient()

# Get current date
current_date = datetime.now().strftime("%Y_%m_%d")

# Define path to save the data
if system == "Windows":
    local_dir_out = "D:/Dropbox/Panama_Data/IMHPA/raw"
elif system == "Linux":
    local_dir_out = "/home/gaby/Dropbox/Panama_Data/IMHPA/raw"
else:
    raise EnvironmentError(f"Unsupported operating system: {system}")

logger.info(f"Running on {system}. Output directory: {local_dir_out}")

if not os.path.exists(local_dir_out):
    os.makedirs(local_dir_out)

# Get list of sensors
all_sensors = client.get_sensors()

#  Loop through sensors and stations to retrieve and save data
for sensor in all_sensors:
    logger.info(f"Processing sensor: {sensor}")

    stations_df = client.get_stations(sensor=sensor)
    logger.info(f"Found {len(stations_df)} stations for sensor {sensor}")

    output_path = f"{local_dir_out}/{sensor}"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    csv_output = f"{output_path}/{sensor.lower()}_stations.csv"
    if not os.path.exists(csv_output):
        stations_df.to_csv(csv_output, index=False)
        
    list_of_dfs = []
    for station_id in stations_df['id']:
        logger.info(f"Processing station: {station_id}")
        data_df = client.get_data(sensor=sensor, station_id=station_id)
        list_of_dfs.append(data_df)
    
    final_df = pd.concat(list_of_dfs, ignore_index=True)
    
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    final_df["ingest_timestamp"] = current_timestamp
    
    file_name = f"{sensor}_{current_date}.parquet"
    file_path = f"{output_path}/{file_name}"
    final_df.to_parquet(file_path)

