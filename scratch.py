# Scratch

#%%

# Load libraries
import re
import json
import pandas as pd
from pathlib import Path
from imhpa import ImhpaClient

#%%

# Part 1: Calculate total number of pairs from server (today)

client = ImhpaClient()

my_sensors = client.get_sensors()

total = 0
for sensor_id in my_sensors['code']:
    my_stations = client.list_stations(sensor=sensor_id)
    num_stations = len(my_stations)
    # print(sensor_id)
    # print("\tNumber of stations: ", num_stations)
    total += num_stations

print("Total number of (sensor, station) pairs: ", total)
#%%

# Part 2: Calculate total number of pairs in csv files

## Define data root
DATA_ROOT = Path("D:/Dropbox/Panama_Data/IMHPA")

## Check path of list of sensors
csv_files = list(DATA_ROOT.rglob("raw/**/*.csv"))

## Create dictionary of sensors and stations
sensors_dict = {}
for file in csv_files:
    sensor = str(file).split("\\")[-2]
    stations = list(pd.read_csv(file)['id'])

    sensors_dict[sensor] = {}
    sensors_dict[sensor]['stations'] = stations
    sensors_dict[sensor]['n_stations'] = len(stations)

## Count number of sensors and stations in csv file
stations_df = pd.DataFrame(sensors_dict).T
num_stations = stations_df['n_stations'].sum()
print("Total number of pairs in raw folder: ", num_stations)

#%%

# Part 3: Create sensor-station manifest of every ingestion in RAW

## Read all parquet files and select relevant columns
parquet_files = list(DATA_ROOT.rglob("raw/**/*.parquet"))
df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)
df = df[['sensor', 'station_id', 'ingest_timestamp']].drop_duplicates()

## Map sensor name to correct code
sensor_mapping = {
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
    "Presión barométrica": "P_BAROM",
    "Punto de Rocío": "P_ROCIO",
    "Radiación Solar": "RAD_SOLAR",
    "Velocidad Máxima del Viento": "RAFAGA",
    "Relación de Mezcla": "R_MEZCLA",
    "Temperatura Promedio": "TEMP_PROM",
    "Velocidad del Viento a 10m": "VEL_VIENTO-VEL_VIENTO10",
    "Velocidad del Viento a 2m": "VEL_VIENTO2"
}
df['sensor'] = df['sensor'].map(sensor_mapping)

## Create ingestion date column
df['ingest_timestamp'] = pd.to_datetime(df['ingest_timestamp'])
df['ingestion_date'] = df['ingest_timestamp'].dt.date

## Extract unique sensor-station pairs per ingestion timestamp
file_name = "ingestion_manifest.json"
my_cols = ['sensor', 'station_id']
for date in df['ingestion_date'].unique():
    subset = df.loc[df['ingestion_date'] == date, my_cols].copy()
    sensor_station_pairs = [tuple(row) for row in subset.to_numpy()]
    my_dict = {"ingestion_date": str(date), 
               "number_of_pairs": len(sensor_station_pairs),
               "sensor_station_pairs": sensor_station_pairs}
    with open(DATA_ROOT / "raw" / "_manifests" / f"{date}_{file_name}", "w") as f:
        json.dump(my_dict, f, indent=4)


#%%

# Part 3: Create sensor-station manifest of every ingestion in BRONZE

# Define bronze root
BRONZE_ROOT = DATA_ROOT / "bronze"

# Define function to extract sensor name from file path
def extract_sensor(path):
    stem = path.stem
    stem = re.sub(r"^data_\d+_", "", stem)        ## drop 'data_{station_id}_'
    stem = re.sub(r"_\d{8}T\d{6}Z$", "", stem)    ## drop '_20260328T162547Z'
    return stem

# Define function to extract station id from file path
def extract_station_id(path):
    stem = path.stem
    stem = re.sub(r"^data_", "", stem)              ## drop 'data_'
    stem = re.sub(r"_.+_\d{8}T\d{6}Z$", "", stem)   ## drop '_SENSOR_timestamp'
    return stem

# Extract all ingestion folders in bronze root
bronze_ingestion_folders = []
for folder_path in list(BRONZE_ROOT.glob("*")):
    date_folder = str(folder_path).split("\\")[-1]
    bronze_ingestion_folders.append(date_folder)

# Loop through all ingestion folders and create manifest for each
for ingestion_date in bronze_ingestion_folders:
    ingestion_batch = list((BRONZE_ROOT / ingestion_date).glob("data*.json"))

    sensor_station_pairs = []
    for file_path in ingestion_batch:
        sensor = extract_sensor(file_path)
        station_id = extract_station_id(file_path)
        sensor_station_pairs.append((sensor, station_id))

    my_date = ingestion_date.split("=")[-1]
        
    my_dict = {
        "ingestion_date": my_date,
        "number_of_pairs": len(sensor_station_pairs),
        "sensor_station_pairs": sensor_station_pairs
        }

    file_name = f"_ingestion_manifest_{my_date}.json"
    with open(BRONZE_ROOT / ingestion_date / file_name, "w") as f:
        json.dump(my_dict, f, indent=4)


# %%
