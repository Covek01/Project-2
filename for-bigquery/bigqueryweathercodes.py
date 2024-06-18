import os
from google.cloud import bigquery
import pandas as pd


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bpl-stadiums.json'
client = bigquery.Client()
project_id = 'bpl-stadiums'
dataset_id = 'bpl-stadiums.day_measures'
table_id = "bpl-stadiums.day_measures.weather_codes"

df = pd.read_csv('../dims/weather_codes.csv')
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
client.load_table_from_dataframe(df, table_id, job_config=job_config)
print(f"Data loaded into {project_id}.{dataset_id}.{table_id}")