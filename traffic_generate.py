import random
import time
import json
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

CONNECTION_STR = "YOUR_KEY_HERE"

EVENT_HUB_NAME = "mariam-hub"

locations = [
    "Cairo-RingRoad",
    "Alex-Corniche",
    "Giza-Square",
    "Nasr-City",
    "6October-Bridge",
]
weather_conditions = ["Sunny", "Cloudy", "Rainy", "Foggy", "Windy"]


def generate_traffic_data():
    timestamp = datetime.now().isoformat()
    location = random.choice(locations)
    car_count = random.randint(0, 20)
    truck_count = random.randint(0, 10)
    bike_count = random.randint(0, 15)
    traffic_volume = car_count + truck_count + bike_count
    avg_speed = random.randint(20, 120)
    weather = random.choice(weather_conditions)
    data = {
        "timestamp": timestamp,
        "location": location,
        "traffic_volume": traffic_volume,
        "avg_speed": avg_speed,
        "car_count": car_count,
        "truck_count": truck_count,
        "bike_count": bike_count,
        "weather": weather,
    }
    return data


if __name__ == "__main__":
    print(
        "Starting live traffic data stream to Azure Event Hub (press CTRL+C to stop)\n"
    )

    producer = EventHubProducerClient.from_connection_string(
        conn_str="YOUR", eventhub_name="YOUR_NAME"
    )

    try:
        while True:
            traffic_data = generate_traffic_data()
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(traffic_data)))
            producer.send_batch(batch)
            print(f"Sent: {traffic_data}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStream stopped by user")
