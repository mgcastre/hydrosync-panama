# Scratch

# from imhpa import ImhpaClient

# client = ImhpaClient()

# my_sensors = client.get_sensors()

# total = 0
# for sensor_id in my_sensors['code']:
#     my_stations = client.list_stations(sensor=sensor_id)
#     num_stations = len(my_stations)
#     print(sensor_id)
#     print("\tNumber of stations: ", num_stations)
#     total += num_stations

#%%

# Load libraries
import pandas as pd
from pathlib import Path

# Define data root
DATA_ROOT = Path("D:/Dropbox/Panama_Data/IMHPA")

# Check path of list of sensors
csv_files = list(DATA_ROOT.rglob("raw/**/*.csv"))

#%%

# Create dictionary of sensors and stations
sensors_dict = {}
for file in csv_files:
    sensor = str(file).split("\\")[-2]
    stations = list(pd.read_csv(file)['id'])
    sensors_dict[sensor] = stations

#%%

# Folders in bronze layer
BRONZE_ROOT = DATA_ROOT / "bronze"

ingestion_dates = []
for path in list(BRONZE_ROOT.glob("*")):
    my_date = str(path).split("\\")[-1]
    ingestion_dates.append(my_date)

# Extract all sensors and stations in first ingestion batch
bronze_folder = BRONZE_ROOT / ingestion_dates[0]
ingestion_batch = list(bronze_folder.glob("*LLUVIA*.json"))

for full_path in ingestion_batch:
    file_name = str(path).split("\\")[-1]
    station = file_name.split("_")[1]


# %%
