from pyspark.sql import SparkSession
import csv
from pyspark.sql.functions import col, expr, lag, udf, window, avg, last, sum
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.streaming import state
HOST = 'localhost'
PORT = 10000
FORMAT = 'socket'


def read_teams_create_dict(filepath):
    teams_name_id = dict()
    with open(filepath, mode='r', newline='\n') as csvfile:
        csv_reader = csv.DictReader(csvfile)

        for row in csv_reader:
            teams_name_id[row['team_name']] = row['id']
    return teams_name_id


if __name__ == '__main__':
    teams_dict = read_teams_create_dict('./dims/teams.csv')

    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('FootballWeatherDataStreaming') \
        .getOrCreate()



    socket_df = spark.readStream \
        .format(FORMAT) \
        .option('host', HOST) \
        .option('port', PORT) \
        .load()
    
    for i in range(1,len(teams_dict.keys())):
        temp_df=spark.readStream\
        .format(FORMAT)\
        .option('host',HOST)\
        .option('port',PORT+i)\
        .load()
        socket_df=socket_df.unionByName(temp_df,allowMissingColumns=True)

    parsed_df = socket_df.selectExpr("split(value, ',') AS data")
 
    parsed_df = parsed_df.selectExpr(
        "data[0] as ts",
        "CASE WHEN data[1] = 'null' THEN NULL ELSE CAST(data[1] AS DOUBLE) END as temperature",
        "CASE WHEN data[2] = 'null' THEN NULL ELSE CAST(data[2] AS DOUBLE) END as humidity",
        "CASE WHEN data[3] = 'null' THEN NULL ELSE CAST(data[3] AS DOUBLE) END as dew_point",
        "CASE WHEN data[4] = 'null' THEN NULL ELSE CAST(data[4] AS DOUBLE) END as apparent_temperature",
        "CASE WHEN data[5] = 'null' THEN NULL ELSE CAST(data[5] AS DOUBLE) END as precipitation",
        "CASE WHEN data[6] = 'null' THEN NULL ELSE CAST(data[6] AS DOUBLE) END as rain",
        "CASE WHEN data[7] = 'null' THEN NULL ELSE CAST(data[7] AS DOUBLE) END as snowfall",
        "CASE WHEN data[8] = 'null' THEN NULL ELSE CAST(data[8] AS DOUBLE) END as snow_depth",
        "CASE WHEN data[9] = 'null' THEN NULL ELSE CAST(data[9] AS INT) END as weather_code",
        "CASE WHEN data[10] = 'null' THEN NULL ELSE CAST(data[10] AS DOUBLE) END as pressure_sea_level",
        "CASE WHEN data[11] = 'null' THEN NULL ELSE CAST(data[11] AS DOUBLE) END as pressure_surface",
        "CASE WHEN data[12] = 'null' THEN NULL ELSE CAST(data[12] AS INT) END as cloud_coverage",
        "CASE WHEN data[13] = 'null' THEN NULL ELSE CAST(data[13] AS INT) END as cloud_coverage_low",
        "CASE WHEN data[14] = 'null' THEN NULL ELSE CAST(data[14] AS INT) END as cloud_coverage_mid",
        "CASE WHEN data[15] = 'null' THEN NULL ELSE CAST(data[15] AS INT) END as cloud_coverage_high",
        "CASE WHEN data[16] = 'null' THEN NULL ELSE CAST(data[16] AS DOUBLE) END as evaporation",
        "CASE WHEN data[17] = 'null' THEN NULL ELSE CAST(data[17] AS DOUBLE) END as vapour_pressure_deficit",
        "CASE WHEN data[18] = 'null' THEN NULL ELSE CAST(data[18] AS DOUBLE) END as wind_speed_10m",
        "CASE WHEN data[19] = 'null' THEN NULL ELSE CAST(data[19] AS DOUBLE) END as wind_speed_100m",
        "CASE WHEN data[20] = 'null' THEN NULL ELSE CAST(data[20] AS INT) END as wind_direction_10m",
        "CASE WHEN data[21] = 'null' THEN NULL ELSE CAST(data[21] AS INT) END as wind_direcction_100m",
        "CASE WHEN data[22] = 'null' THEN NULL ELSE CAST(data[22] AS DOUBLE) END as wind_gusts_10m",
        "CASE WHEN data[23] = 'null' THEN NULL ELSE CAST(data[23] AS DOUBLE) END as soil_temperature",
        "data[24] as team_id",
    )


    # parsed_df = parsed_df.filter(col('temperature').rlike('^-?\\d+(\\.\\d+)?$'))

    parsed_df = parsed_df.withColumn(
        'ts', F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))

    parsed_df = (parsed_df.withColumn('team_id', col('team_id').cast(
        'string')).replace(teams_dict, subset=['team_id']))

###################
    parsed_df.createOrReplaceTempView("updates")    
    another_df=spark.sql("select ts,temperature,team_id from updates")
###################


    parsed_df.printSchema()
    windowed_df=parsed_df.withWatermark('ts','2 hours').groupBy(window(parsed_df.ts,'2 hours','1 hour'),parsed_df.team_id).agg(avg('temperature').alias('sumirano'))

    query = windowed_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()
    query.awaitTermination()
