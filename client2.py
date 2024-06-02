import sys
import socket
import time
import threading
import requests
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark


HOST = 'localhost'
PORT = 10000

def start_client(host: str, port: int):
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #     sock.connect((host, port))
    #     while True:
    #         data =  sock.recv(4096)
    #         if not data:
    #             break

    spark = SparkSession.builder.master("local[2]") \
                .appName("AirRadarSerbia").getOrCreate()
    socketDFs = list()
    parsed_dfs = list()
    parsedDFs = list()
    for i in range(0, 20):
        socketDF = spark.readStream \
            .format("socket") \
            .option("host", host) \
            .option("port", port + i) \
            .load()
        socketDFs.append(socketDF)

        parsed_df = socketDF.selectExpr("split(value, ',') AS data")
        parsed_dfs.append(parsed_df)
        parsedDFs.append(parsed_df.selectExpr(
            "data[0] as IndexLabel",
            "CASE WHEN data[1] = 'null' THEN NULL ELSE CAST(data[1] AS DOUBLE) END as temperature_2m",
            "CASE WHEN data[2] = 'null' THEN NULL ELSE CAST(data[2] AS DOUBLE) END as relative_humidity_2m",
            "CASE WHEN data[3] = 'null' THEN NULL ELSE CAST(data[3] AS DOUBLE) END as dew_point_2m",
            "CASE WHEN data[4] = 'null' THEN NULL ELSE CAST(data[4] AS DOUBLE) END as apparent_temperature",
            "CASE WHEN data[5] = 'null' THEN NULL ELSE CAST(data[5] AS DOUBLE) END as precipitation",
            "CASE WHEN data[6] = 'null' THEN NULL ELSE CAST(data[6] AS DOUBLE) END as rain",
            "CASE WHEN data[7] = 'null' THEN NULL ELSE CAST(data[7] AS DOUBLE) END as snowfall",
            "CASE WHEN data[8] = 'null' THEN NULL ELSE CAST(data[8] AS DOUBLE) END as snow_depth",
            "CAST(data[9] AS INT) as weather_code",
            "CASE WHEN data[10] = 'null' THEN NULL ELSE CAST(data[10] AS DOUBLE) END as pressure_msl",
            "CASE WHEN data[11] = 'null' THEN NULL ELSE CAST(data[11] AS DOUBLE) END as surface_pressure",
            "CASE WHEN data[12] = 'null' THEN NULL ELSE CAST(data[12] AS DOUBLE) END as cloud_cover",
            "CASE WHEN data[13] = 'null' THEN NULL ELSE CAST(data[13] AS DOUBLE) END as cloud_cover_low",
            "CASE WHEN data[14] = 'null' THEN NULL ELSE CAST(data[14] AS DOUBLE) END as cloud_cover_mid",
            "CASE WHEN data[15] = 'null' THEN NULL ELSE CAST(data[15] AS DOUBLE) END as cloud_cover_high",
            "CASE WHEN data[16] = 'null' THEN NULL ELSE CAST(data[16] AS DOUBLE) END as et0_fao_evapotranspiration",
            "CASE WHEN data[17] = 'null' THEN NULL ELSE CAST(data[17] AS DOUBLE) END as vapour_pressure_deficit",
            "CASE WHEN data[18] = 'null' THEN NULL ELSE CAST(data[18] AS DOUBLE) END as wind_speed_10m",
            "CASE WHEN data[19] = 'null' THEN NULL ELSE CAST(data[19] AS DOUBLE) END as wind_speed_100m",
            "CASE WHEN data[20] = 'null' THEN NULL ELSE CAST(data[20] AS DOUBLE) END as wind_direction_10m",
            "CASE WHEN data[21] = 'null' THEN NULL ELSE CAST(data[21] AS DOUBLE) END as wind_direction_100m",
            "CASE WHEN data[22] = 'null' THEN NULL ELSE CAST(data[22] AS DOUBLE) END as wind_gusts_10m",
            "CASE WHEN data[23] = 'null' THEN NULL ELSE CAST(data[23] AS DOUBLE) END as soil_temperature_0_to_7cm",
            "data[24] as team_name"
        ))




    queries = list()
    for parsedDF in parsedDFs:
        query = parsedDF.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
        queries.append(query)

    for query in queries:
        query.awaitTermination()


    # parsedDF_filtered = parsedDF\
    #              .where("latitude > 60 OR velocity > 800")
    #             # .avg("geo_altitude", "timestamp")




if __name__ == "__main__":
    # arguments = sys.argv

    # if (len(arguments) < 3):
    #     print("LESS THAN 2 ARGUMENTS")
    
    host = 'localhost' 
    port = 9999  
    start_client(host, port)