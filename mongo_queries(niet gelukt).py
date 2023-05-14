from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta

uri = "mongodb+srv://source-lberv:cZNUvo1IbemTQRQm@source-weather.opey9ww.mongodb.net/?retryWrites=true&w=majority"
mongo_client = MongoClient(uri, server_api=ServerApi('1'))
db = mongo_client.Source_Weather
collection = db.weather_data

# Expose the latest weather conditions
latest_weather = collection.find_one(sort=[('timestamp', -1)])
print('Latest weather conditions:')
print(latest_weather)

# Expose the development of the weather parameters over the last 24h in 15 min increments
start_time = datetime.utcnow() - timedelta(hours=24)
hourly_data = collection.aggregate([
    {
        '$match': {
            'timestamp': {'$gte': start_time}
        }
    },
    {
        '$addFields': {
            'hour': {'$hour': '$timestamp'},
            'minute': {'$minute': '$timestamp'}
        }
    },
    {
        '$group': {
            '_id': {'hour': '$hour', 'minute': {'$subtract': ['$minute', {'$mod': ['$minute', 15]}]}},
            'temperature': {'$avg': '$external_temperature_c'},
            'wind_speed_unmuted': {'$avg': '$wind_speed_unmuted_m_s'},
            'wind_speed': {'$avg': '$wind_speed_m_s'},
            'wind_direction': {'$avg': '$wind_direction_degrees'},
            'humidity': {'$avg': '$relative_humidity_perc'}
        }
    },
    {
        '$sort': {
            '_id': 1
        }
    }
])
print('Development of weather parameters over the last 24h in 15 min increments:')
for hour_data in hourly_data:
    print(hour_data)

# Expose the average for each of the weather parameters for the last 24h
avg_data_24h = collection.aggregate([
    {
        '$match': {
            'timestamp': {'$gte': start_time}
        }
    },
    {
        '$group': {
            '_id': None,
            'temperature': {'$avg': '$external_temperature_c'},
            'wind_speed_unmuted': {'$avg': '$wind_speed_unmuted_m_s'},
            'wind_speed': {'$avg': '$wind_speed_m_s'},
            'wind_direction': {'$avg': '$wind_direction_degrees'},
            'humidity': {'$avg': '$relative_humidity_perc'}
        }
    }
])
print('Average for each of the weather parameters for the last 24h:')
for data in avg_data_24h:
    print(data)

# Expose the development of the weather parameters over the last 7 days in 1 day increments (average per day)
start_time = datetime.utcnow() - timedelta(days=7)
daily_data = collection.aggregate([
    {
        '$match': {
            'timestamp': {'$gte': start_time}
        }
    },
    {
        '$addFields': {
            'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$timestamp'}}
        }
    },
    {
        '$group': {
            '_id': '$date',
            'temperature': {'$avg': '$external_temperature_c'},
            'wind_speed_unmuted': {'$avg': '$wind_speed_unmuted_m_s'},
            'wind_speed': {'$avg': '$wind_speed_m_s'},
            'wind_direction': {'$avg': '$wind_direction_degrees'},
            'humidity': {'$avg': '$relative_humidity_perc'}
        }
    },
    {
        '$sort': {
            '_id': 1
        }
    }
])

result = list(collection.find({
    "timestamp": {
        "$gte": "2021-05-14 00:00:00",
        "$lt": "2021-05-15 00:00:00"
    }
}))
print(result)