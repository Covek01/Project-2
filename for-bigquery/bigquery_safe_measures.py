import os
from google.cloud import bigquery
import pandas as pd


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bpl-stadiums.json'
client = bigquery.Client()
project_id = 'bpl-stadiums'
dataset_id = 'bpl-stadiums.day_measures'
table_id = "bpl-stadiums.day_measures.safe_to_play_measures"

columns = [
    'time',
    'temperature',
    'rel_humidity',
    'apparent_temperature',
    'precipitation',
    'rain',
    'snowfall',
    'snow_depth',
    'weather_code',
    'cloud_cover',
    'cloud_cover_low',
    'cloud_cover_mid',
    'cloud_cover_high',
    'wind_speed_10m',
    'wind_speed_100m',
    'wind_direction_10m',
    'wind_direction_100m',
    'wind_gusts_10m',
    'soil_temperature',
    'team_id',
    'safe'
]

path = '../output_hours'
lista = os.listdir(path)
final_files = []
for f in lista:
    base_name, extension = os.path.splitext(f)
    filepath = f'{path}/{f}'
    if (extension == '.csv' and os.stat(filepath).st_size > 0):
        final_files.append(filepath)

if len(final_files) > 0:
    df = pd.read_csv(final_files[0], header = None, names = columns)
    df['time'] = pd.to_datetime(df['time'], format='ISO8601')

        
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    client.load_table_from_dataframe(df, table_id, job_config=job_config)
    print(f"Data file loaded into {project_id}.{dataset_id}.{table_id}")


    for file_path in final_files[1:]:
        df = pd.read_csv(file_path, header = None, names = columns)
        df['time'] = pd.to_datetime(df['time'], format='ISO8601')

        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        client.load_table_from_dataframe(df, table_id, job_config=job_config)
        print(f"Data file loaded into {project_id}.{dataset_id}.{table_id}")
        

    print(f"Full data loaded into {project_id}.{dataset_id}.{table_id}")