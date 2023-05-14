from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta
import math
from collections import defaultdict

uri = "mongodb+srv://source-lberv:cZNUvo1IbemTQRQm@source-weather.opey9ww.mongodb.net/?retryWrites=true&w=majority"
mongo_client = MongoClient(uri, server_api=ServerApi('1'))
db = mongo_client.Source_Weather
collection = db.weather_data

# Expose the latest weather conditions
latest_weather = collection.find_one(sort=[('timestamp', -1)])
print('Latest weather conditions:')
print(f'Timestamp: {latest_weather["timestamp"]}')
print(f'External Temperature (C): {latest_weather["external_temperature_c"]}')
print(f'Wind Speed (Unmuted, m/s): {latest_weather["wind_speed_unmuted_m_s"]}')
print(f'Wind Speed (Muted, m/s): {latest_weather["wind_speed_m_s"]}')
print(f'Wind Direction (Degrees): {latest_weather["wind_direction_degrees"]}')
print(f'Relative Humidity (%): {latest_weather["relative_humidity_perc"]}')
print("\n")

# Expose the development of the weather parameters over the last 24h in 15 min increments
#normally this would be used, but there is no available data for the last 24 hours
#start_time = datetime.utcnow() - timedelta(hours=24)
#end_time = start_time + timedelta(days=1)

start_time = '2021-05-14 00:00:00' 
end_time = '2021-05-15 00:00:00'

result_24h = list(collection.find({
    "timestamp": {
        "$gte": start_time,
        "$lt": end_time
    }
}))

start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
minute = math.ceil(start_time.minute / 15) * 15
start_time = start_time.replace(minute=minute, second=0, microsecond=0)

# Loop through 15 minute intervals and find the closest timestamp
while start_time < end_time:
    # Calculate the end time of the current 15 minute interval
    end_of_interval = start_time + timedelta(minutes=15)
    closest_timestamp = None
    closest_diff = timedelta.max

    # Loop through the data and find the closest timestamp to the current interval
    for row in result_24h:
        timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
        if start_time <= timestamp < end_of_interval:
            diff = abs(timestamp - start_time)
            if diff < closest_diff:
                closest_timestamp = timestamp
                closest_diff = diff

    # Print the data for the closest timestamp if it exists
    if closest_timestamp:
        closest_data = next((row for row in result_24h if row['timestamp'] == closest_timestamp.strftime('%Y-%m-%d %H:%M:%S')), None)
        if closest_data:
            print(f'Timestamp: {closest_data["timestamp"]}')
            print(f'External Temperature (C): {closest_data["external_temperature_c"]}')
            print(f'Wind Speed (Unmuted, m/s): {closest_data["wind_speed_unmuted_m_s"]}')
            print(f'Wind Speed (Muted, m/s): {closest_data["wind_speed_m_s"]}')
            print(f'Wind Direction (Degrees): {closest_data["wind_direction_degrees"]}')
            print(f'Relative Humidity (%): {closest_data["relative_humidity_perc"]}')

    start_time = end_of_interval

#set variables to 0 to be able to add to sum
external_temperature_c_sum = 0
wind_speed_unmuted_m_s_sum = 0
wind_speed_m_s_sum = 0
wind_direction_degrees_sum = 0
relative_humidity_perc_sum = 0

for row in result_24h:
    external_temperature_c_sum += row['external_temperature_c']
    wind_speed_unmuted_m_s_sum += row['wind_speed_unmuted_m_s']
    wind_speed_m_s_sum += row['wind_speed_m_s']
    wind_direction_degrees_sum += row['wind_direction_degrees']
    relative_humidity_perc_sum += row['relative_humidity_perc']

count = len(result_24h)
average_external_temperature_c = external_temperature_c_sum / count
average_wind_speed_unmuted_m_s = wind_speed_unmuted_m_s_sum / count
average_wind_speed_m_s = wind_speed_m_s_sum / count
average_wind_direction_degrees = wind_direction_degrees_sum / count
average_relative_humidity_perc = relative_humidity_perc_sum / count

print('Average weather parameters for the last 24 hours:')
print(f'External Temperature (C): {round(average_external_temperature_c,2)}')
print(f'Wind Speed (Unmuted, m/s): {round(average_wind_speed_unmuted_m_s,2)}')
print(f'Wind Speed (Muted, m/s): {round(average_wind_speed_m_s,2)}')
print(f'Wind Direction (Degrees): {round(average_wind_direction_degrees, 2)}')
print(f'Relative Humidity (%): {round(average_relative_humidity_perc,2)}')
print("\n")

#7 days
#normally this would be used, but there is no available data for the last 7 days
#end_time = datetime.utcnow() - timedelta(hours=24)
#start_time = end_time - timedelta(days=7)
start_time = '2021-05-07 00:00:00' 
end_time = '2021-05-15 00:00:00'

result_7d = list(collection.find({
    "timestamp": {
        "$gte": start_time,
        "$lt": end_time
    }
}))

#correct date type for times
start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

# Create a dictionary to hold the average weather parameters for each day
daily_averages = defaultdict(dict)

# Loop through each day and calculate the average weather parameters for that day
for i in range(7):
    # Calculate the start and end times for the current day
    day_start = start_time + timedelta(days=i)
    day_end = day_start + timedelta(days=1)

    # Find all weather data for the current day
    day_data = [row for row in result_7d if day_start <= datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S') < day_end]

    # Calculate the average weather parameters for the current day
    if day_data:
        external_temperature_c_sum = 0
        wind_speed_unmuted_m_s_sum = 0
        wind_speed_m_s_sum = 0
        wind_direction_degrees_sum = 0
        relative_humidity_perc_sum = 0

        for row in day_data:
            external_temperature_c_sum += row['external_temperature_c']
            wind_speed_unmuted_m_s_sum += row['wind_speed_unmuted_m_s']
            wind_speed_m_s_sum += row['wind_speed_m_s']
            wind_direction_degrees_sum += row['wind_direction_degrees']
            relative_humidity_perc_sum += row['relative_humidity_perc']

        count = len(day_data)
        daily_averages[day_start.date()] = {
            'external_temperature_c': round(external_temperature_c_sum / count, 2),
            'wind_speed_unmuted_m_s': round(wind_speed_unmuted_m_s_sum / count, 2),
            'wind_speed_m_s': round(wind_speed_m_s_sum / count, 2),
            'wind_direction_degrees': round(wind_direction_degrees_sum / count, 2),
            'relative_humidity_perc': round(relative_humidity_perc_sum / count, 2),
        }

# Print the daily average weather parameters for the last 7 days
print('Daily average weather parameters for the last 7 days:')
for date, averages in daily_averages.items():
    print(f'Date: {date.strftime("%Y-%m-%d")}')
    print(f'External Temperature (C): {averages["external_temperature_c"]}')
    print(f'Wind Speed (Unmuted, m/s): {averages["wind_speed_unmuted_m_s"]}')
    print(f'Wind Speed (Muted, m/s): {averages["wind_speed_m_s"]}')
    print(f'Wind Direction (Degrees): {averages["wind_direction_degrees"]}')
    print(f'Relative Humidity (%): {averages["relative_humidity_perc"]}')
    print("\n")

for row in result_7d:
    external_temperature_c_sum += row['external_temperature_c']
    wind_speed_unmuted_m_s_sum += row['wind_speed_unmuted_m_s']
    wind_speed_m_s_sum += row['wind_speed_m_s']
    wind_direction_degrees_sum += row['wind_direction_degrees']
    relative_humidity_perc_sum += row['relative_humidity_perc']

count = len(result_7d)
average_external_temperature_c = external_temperature_c_sum / count
average_wind_speed_unmuted_m_s = wind_speed_unmuted_m_s_sum / count
average_wind_speed_m_s = wind_speed_m_s_sum / count
average_wind_direction_degrees = wind_direction_degrees_sum / count
average_relative_humidity_perc = relative_humidity_perc_sum / count

print('Average weather parameters for the last 7 days:')
print(f'External Temperature (C): {round(average_external_temperature_c,2)}')
print(f'Wind Speed (Unmuted, m/s): {round(average_wind_speed_unmuted_m_s,2)}')
print(f'Wind Speed (Muted, m/s): {round(average_wind_speed_m_s,2)}')
print(f'Wind Direction (Degrees): {round(average_wind_direction_degrees, 2)}')
print(f'Relative Humidity (%): {round(average_relative_humidity_perc,2)}')