MAX_ATEMP = 33
MIN_ATEMP = 0
MAX_PRECIPITATION = 4
MAX_WIND_SPEED = 40
MAX_WIND_GUSTS = 65
MIN_SOIL_TEMPERATURE = 1
MAX_SOIL_TEMPERATURE = 35
MAX_SNOW_DEPTH = 0.03

def calculate_formula(snow, soil, windspeed, windgust, atemp, precipitation):
    factor_temp = abs(((MAX_ATEMP + MIN_ATEMP) / 2 - atemp)) / (MAX_ATEMP - MIN_ATEMP)
    factor_snow = snow / MAX_SNOW_DEPTH
    factor_soil = abs(((MAX_SOIL_TEMPERATURE + MIN_SOIL_TEMPERATURE) / 2 - soil)) / (MAX_SOIL_TEMPERATURE - MIN_SOIL_TEMPERATURE)
    windspeed_factor = windspeed / MAX_WIND_SPEED
    windgust_factor = windgust / MAX_WIND_GUSTS
    precipitation_factor = precipitation / MAX_PRECIPITATION

    return factor_temp + factor_snow + factor_soil + windspeed_factor + windgust_factor + precipitation_factor <= 3