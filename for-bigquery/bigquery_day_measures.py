import os
from google.cloud import bigquery
import pandas as pd


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bpl-stadiums.json'
client = bigquery.Client()
project_id = 'bpl-stadiums'
dataset_id = 'bpl-stadiums.day_measures'
table_id = "bpl-stadiums.day_measures.day_measures_table"

columns = [
    'team_id',
    'avg_temp',
    'max_temp',
    'min_temp',
    'avg_humidity',
    'avg_apparent_temp',
    'max_apparent_temp',
    'min_apparent_temp',
    'avg_precipitation',
    'max_precipitation',
    'avg_rain',
    'avg_snowfall',
    'avg_snowdepth',
    'max_snowdepth',
    'avg_cloud_coverage',
    'avg_wind_speed_10m',
    'avg_wind_speed_100m',
    'avg_wind_direction_10m',
    'avg_wind_direction_100m',
    'avg_wind_gusts_10m',
    'soil_temperature',
    'window_start',
    'window_end'
]

path = '../output_daily'
lista = os.listdir(path)
final_files = []
for f in lista:
    base_name, extension = os.path.splitext(f)
    filepath = f'{path}/{f}'
    if (extension == '.csv' and os.stat(filepath).st_size > 0):
        final_files.append(filepath)

if len(final_files) > 0:
    df = pd.read_csv(final_files[0], header = None, names = columns)
    df['window_start'] = pd.to_datetime(df['window_start'], format='ISO8601')
    df['window_end'] = pd.to_datetime(df['window_end'], format='ISO8601')
        
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    client.load_table_from_dataframe(df, table_id, job_config=job_config)
    print(f"Data file loaded into {project_id}.{dataset_id}.{table_id}")


    for file_path in final_files[1:]:
        df = pd.read_csv(file_path, header = None, names = columns)
        df['window_start'] = pd.to_datetime(df['window_start'], format='ISO8601')
        df['window_end'] = pd.to_datetime(df['window_end'], format='ISO8601')
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        client.load_table_from_dataframe(df, table_id, job_config=job_config)
        print(f"Data file loaded into {project_id}.{dataset_id}.{table_id}")
        

    print(f"Full data loaded into {project_id}.{dataset_id}.{table_id}")