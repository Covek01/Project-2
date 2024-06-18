from pyspark.sql import SparkSession
import csv
from pyspark.sql.functions import col, expr, lag, udf, window, avg, last, sum
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.streaming import state
from pyspark.sql.functions import when, abs
from limits import MAX_ATEMP, MAX_SNOW_DEPTH, MAX_PRECIPITATION, MAX_WIND_GUSTS, MAX_WIND_SPEED, MIN_SOIL_TEMPERATURE, MIN_ATEMP, MAX_SOIL_TEMPERATURE, calculate_formula
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

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
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    socket_df = spark.readStream \
        .format(FORMAT) \
        .option('host', HOST) \
        .option('port', PORT) \
        .load()
    for i in range(1, len(teams_dict.keys())):
        temp_df = spark.readStream\
            .format(FORMAT)\
            .option('host', HOST)\
            .option('port', PORT+i)\
            .load()
        socket_df = socket_df.unionByName(temp_df, allowMissingColumns=True)

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
        "CASE WHEN data[21] = 'null' THEN NULL ELSE CAST(data[21] AS INT) END as wind_direction_100m",
        "CASE WHEN data[22] = 'null' THEN NULL ELSE CAST(data[22] AS DOUBLE) END as wind_gusts_10m",
        "CASE WHEN data[23] = 'null' THEN NULL ELSE CAST(data[23] AS DOUBLE) END as soil_temperature",
        "data[24] as team_id",
    )

    # parsed_df = parsed_df.filter(col('temperature').rlike('^-?\\d+(\\.\\d+)?$'))
    parsed_df = parsed_df.drop('dew_point')
    parsed_df = parsed_df.drop('pressure_sea_level')
    parsed_df = parsed_df.drop('pressure_surface')
    parsed_df = parsed_df.drop('evaporation')
    parsed_df = parsed_df.drop('vapour_pressure_deficit')

    parsed_df = parsed_df.withColumn(
        'ts', F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))

    parsed_df = (parsed_df.withColumn('team_id', col('team_id').cast(
        'string')).replace(teams_dict, subset=['team_id']))

###################
    parsed_df.createOrReplaceTempView("updates")
    another_df = spark.sql("select ts,temperature,team_id from updates")
    parsed_df.printSchema()
###################
    parsed_df=parsed_df.withWatermark('ts','24 hours')
    daily_df = parsed_df.groupBy(window(
        parsed_df.ts, '24 hours', startTime='0 hours'), parsed_df.team_id)\
        .agg(F.avg('temperature').alias('avg_temp'),
             F.max('temperature').alias('max_temp'),
             F.min('temperature').alias('min_temp'),
             F.avg('humidity').alias('avg_humidity'),
             F.avg('apparent_temperature').alias('avg_apparent_temp'),
             F.max('apparent_temperature').alias('max_apparent_temp'),
             F.min('apparent_temperature').alias('min_apparent_temp'),
             F.avg('precipitation').alias('avg_precipitation'),
             F.max('precipitation').alias('max_precipitation'),
             F.avg('rain').alias('avg_rain'),
             F.avg('snowfall').alias('avg_snowfall'),
             F.avg('snow_depth').alias('avg_snowdepth'),
             F.max('snow_depth').alias('max_snow_depth'),
             F.avg('cloud_coverage').alias('avg_cloud_coverage'),
             F.avg('wind_speed_10m').alias('avg_wind_speed_10m'),
             F.avg('wind_speed_100m').alias('avg_wind_speed_100m'),
             F.avg('wind_direction_10m').alias('avg_wind_direction_10m'),
             F.avg('wind_direction_100m').alias('avg_wind_direction_100m'),
             F.avg('wind_gusts_10m').alias('avg_wind_gusts_10m'),
             F.avg('soil_temperature').alias('soil_temperature'))
    
    daily_df=daily_df.withColumn('window_start',daily_df.window.start)
    daily_df=daily_df.withColumn('window_end', daily_df.window.end)
    daily_df=daily_df.drop('window')

    query = daily_df.coalesce(1).writeStream\
        .format("csv")\
        .trigger(processingTime='80 seconds')\
    

    # def calculate_formula2(snow, soil, windspeed, windgust, atemp, precipitation):
    #     factor_temp = abs(((MAX_ATEMP + MIN_ATEMP) / 2 - atemp)) / (MAX_ATEMP - MIN_ATEMP)
    #     factor_snow = snow / MAX_SNOW_DEPTH
    #     factor_soil = abs(((MAX_SOIL_TEMPERATURE + MIN_SOIL_TEMPERATURE) / 2 - soil)) / (MAX_SOIL_TEMPERATURE - MIN_SOIL_TEMPERATURE)
    #     windspeed_factor = windspeed / MAX_WIND_SPEED
    #     windgust_factor = windgust / MAX_WIND_GUSTS
    #     precipitation_factor = precipitation / MAX_PRECIPITATION

    #     return factor_temp + factor_snow + factor_soil + windspeed_factor + windgust_factor + precipitation_factor <= 3
    # calculate_formula_udf = udf(calculate_formula2, BooleanType)

    unsafe_df = parsed_df.filter(
        (parsed_df.snow_depth >= MAX_SNOW_DEPTH) |
        (parsed_df.soil_temperature <= MIN_SOIL_TEMPERATURE) |
        (parsed_df.soil_temperature >= MAX_SOIL_TEMPERATURE) |
        (parsed_df.wind_gusts_10m > MAX_WIND_GUSTS) |
        (parsed_df.wind_speed_10m > MAX_WIND_SPEED) |
        (parsed_df.apparent_temperature <= MAX_ATEMP) |
        (parsed_df.apparent_temperature > MIN_ATEMP) |
        (parsed_df.precipitation >= MAX_PRECIPITATION)
    )

    # parsed_df_with_safe = parsed_df.withColumn('safe_to_play', (when(
    #                             (parsed_df.snow_depth >= MAX_SNOW_DEPTH) |
    #                             (parsed_df.soil_temperature <= MIN_SOIL_TEMPERATURE) |
    #                             (parsed_df.wind_gusts_10m > MAX_WIND_GUSTS) |
    #                             (parsed_df.wind_speed_10m > MAX_WIND_SPEED) |
    #                             (parsed_df.apparent_temperature <= MAX_ATEMP) |
    #                             (parsed_df.apparent_temperature > MIN_ATEMP) |
    #                             (parsed_df.precipitation >= MAX_PRECIPITATION), False)
    #                             .when(calculate_formula_udf(
    #                                 parsed_df.snow_depth,
    #                                 parsed_df.soil_temperature,
    #                                 parsed_df.wind_speed_10m,
    #                                 parsed_df.wind_gusts_10m,
    #                                 parsed_df.apparent_temperature,
    #                                 parsed_df.precipitation
    #                             ), False)
    #                             .otherwise(True))
    #                         )
    
    parsed_df2 = parsed_df.withColumn('safe_to_play', 
        (when(
            (parsed_df.snow_depth >= MAX_SNOW_DEPTH) |
            (parsed_df.soil_temperature <= MIN_SOIL_TEMPERATURE) |
            (parsed_df.wind_gusts_10m > MAX_WIND_GUSTS) |
            (parsed_df.wind_speed_10m > MAX_WIND_SPEED) |
            (parsed_df.apparent_temperature > MAX_ATEMP) |
            (parsed_df.apparent_temperature <= MIN_ATEMP) |
            (parsed_df.precipitation >= MAX_PRECIPITATION) |
            (
                abs(((MAX_ATEMP + MIN_ATEMP) / 2 - parsed_df.apparent_temperature)) / (MAX_ATEMP - MIN_ATEMP) +
                parsed_df.snow_depth / MAX_SNOW_DEPTH +
                abs(((MAX_SOIL_TEMPERATURE + MIN_SOIL_TEMPERATURE) / 2 - parsed_df.soil_temperature)) / (MAX_SOIL_TEMPERATURE - MIN_SOIL_TEMPERATURE) +
                parsed_df.wind_speed_10m / MAX_WIND_SPEED +
                parsed_df.wind_gusts_10m / MAX_WIND_GUSTS +
                parsed_df.precipitation / MAX_PRECIPITATION > 3
            ), False)
            .otherwise(True)
        ))
    



    queryFilter = parsed_df2.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    queryFilter.awaitTermination()


    block_sz = 1024
    query = daily_df.writeStream\
        .outputMode("append")\
        .format("parquet")\
        .option("parquet.block.size", block_sz)\
        .option("checkpointLocation", "./checkpoint")\
        .option('path', './output')\
        .outputMode("append")\
        .start()\
        .awaitTermination()
    # query = daily_df.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    # query.awaitTermination()
    windowed_df=parsed_df.withWatermark('ts','2 hours').groupBy(window(parsed_df.ts,'2 hours','1 hour'),parsed_df.team_id).agg(avg('temperature').alias('sumirano'))


