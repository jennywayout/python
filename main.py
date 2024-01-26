import requests
import influxdb_client
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

bucket = "myWeather"
org = "test" # or email you used to create your Free Tier InfluxDB Cloud account
token = "Ns6HwdKNaNsmVcuS819LHDQWXdEWZumV2_UqDAGBlWQyHTaHtfpD-Kj1ho3hJ1vMO8gpL4BYW698WnehwTPaxQ=="
url = "https://us-east-1-1.aws.cloud2.influxdata.com/" # for example, https://us-east-1.aws.cloud2.influxdata.com/
openWeatherMap_token = "d46ed8ef358bbe73af3d90da3c7845e7"
openWeatherMap_lat = "54.607868"
openWeatherMap_lon = "-5.926437"
openWeather_url = "https://api.openweathermap.org/data/3.0/onecall"

# Get time series data from OpenWeatherMap API
params = {'lat':openWeatherMap_lat, 'lon':openWeatherMap_lon, 'exclude': "minutely,daily", 'appid':openWeatherMap_token}
r = requests.get(openWeather_url, params = params).json()

hourly = r['hourly']

# Convert data to Pandas DataFrame and convert timestamp to datetime object
df = pd.json_normalize(hourly)
df = df.drop(columns=['weather', 'pop'])
df['dt'] = pd.to_datetime(df['dt'], unit='s')
print(df.head)

# Write data to InfluxDB
with InfluxDBClient(url=url, token=token, org=org) as client:
   df = df
   client.write_api(write_options=SYNCHRONOUS).write(bucket=bucket,record=df,
       data_frame_measurement_name="weather",
       data_frame_timestamp_column="dt")