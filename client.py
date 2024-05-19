import sys
import socket
import time
import threading
import requests
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark

def start_client(host: str, port: int):
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #     sock.connect((host, port))
    #     while True:
    #         data =  sock.recv(4096)
    #         if not data:
    #             break

    spark = SparkSession.builder.master("local[2]") \
                .appName("AirRadarSerbia").getOrCreate()
    socketDF = spark.readStream \
            .format("socket") \
            .option("host", host) \
            .option("port", port) \
            .load()
    
    socketDF.printSchema()
    


    
    parsed_df = socketDF.selectExpr("split(value, ', ') AS data")
    
    parsedDF = parsed_df.selectExpr(
        "data[0] as date_time",
        "data[1] as unique_addr",
        "data[2] as callsign",
        "data[3] as country",    
        "CASE WHEN data[4] = 'null' THEN NULL ELSE CAST(data[4] AS DOUBLE) END as latitude",
        "data[5] as on_ground",
        "CASE WHEN data[6] = 'null' THEN NULL ELSE CAST(data[6] AS DOUBLE) END as geo_altitude",
        "CASE WHEN data[7] = 'null' THEN NULL ELSE CAST(data[7] AS DOUBLE) END as velocity",
        "CASE WHEN data[8] = 'null' THEN NULL ELSE CAST(data[8] AS double) END as air_density",
    )

    parsedDF_filtered = parsedDF\
                 .where("latitude > 60 OR velocity > 800")
                # .avg("geo_altitude", "timestamp")

    query = parsedDF_filtered.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    query.awaitTermination()


if __name__ == "__main__":
    # arguments = sys.argv

    # if (len(arguments) < 3):
    #     print("LESS THAN 2 ARGUMENTS")
    
    host = 'localhost' 
    port = 9999  
    start_client(host, port)