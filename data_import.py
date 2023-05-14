import os
from fastapi import FastAPI
from fastapi.testclient import TestClient
import json
from fastapi import FastAPI, Request
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime

uri = "mongodb+srv://source-lberv:cZNUvo1IbemTQRQm@source-weather.opey9ww.mongodb.net/?retryWrites=true&w=majority"

#ip: 85.144.31.181/32 (can add another IP through MongoDB data services for database)
# Create a new client and connect to the server
mongo_client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

app = FastAPI()


@app.post("/")
async def receive_data(request: Request):
    data = await request.json()
    rows = data["rows"]
    for i in rows:
        variable = i[0]
        value = i[1]
        if variable == 'external_temperature_c':
            external_temperature_c = value
        if variable == 'wind_speed_unmuted_m_s':
            wind_speed_unmuted_m_s = value
        if variable == 'wind_speed_m_s':
            wind_speed_m_s = value
        if variable == 'wind_direction_degrees':
            wind_direction_degrees = value
        if variable == 'relative_humidity_perc':
            relative_humidity_perc = value
    timestamp = data["ts"]
    #edit the timestamp so its easier to use in queries
    timestamp_obj = datetime.fromisoformat(timestamp)
    year_month_date_time = timestamp_obj.strftime("%Y-%m-%d %H:%M:%S")

    db = mongo_client.Source_Weather
    collection = db.weather_data

    data = {
        "external_temperature_c": external_temperature_c,
        "wind_speed_unmuted_m_s": wind_speed_unmuted_m_s,
        "wind_speed_m_s": wind_speed_m_s,
        "wind_direction_degrees": wind_direction_degrees,
        "relative_humidity_perc": relative_humidity_perc,
        "timestamp": year_month_date_time
    }

    result = collection.insert_one(data)
    print("Inserted document with id:", result.inserted_id)

    return {"msg": "Data received"}


client = TestClient(app)


def test_receive_data(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    headers = {"Content-Type": "application/json"}
    response = client.post("/", data=json.dumps(data), headers=headers)
    assert response.status_code == 200
    assert response.json() == {"msg": "Data received"}

if __name__ == "__main__":
    # directory containing JSON files (r in front to read as raw string to avoid backslash for escape)
    directory = r"C:\Users\Lucbe\Downloads\may\may"

    # iterate over each file in the directory and its subdirectories and run the test function
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".json"):
                file_path = os.path.join(root, filename)
                test_receive_data(file_path)
