from kafka import KafkaConsumer
import json
import time
from datetime import datetime
from collections import defaultdict

# Initialize Kafka consumer to consume from both topics
consumer = KafkaConsumer(
    "environment-data",
    "traffic-data",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# Aggregation dictionaries for environment and traffic data
daily_environment_data = defaultdict(
    lambda: {
        "temperature": [],
        "humidity": [],
        "pressure": [],
        "air_quality": [],
        "noise_level": [],
    }
)
daily_traffic_data = defaultdict(
    lambda: {"vehicle_count": [], "average_speed": [], "traffic_density": []}
)


# Aggregate environmental data
def aggregate_environment_data(data):
    date = datetime.fromtimestamp(data["timestamp"]).strftime("%Y-%m-%d")
    daily_environment_data[date]["temperature"].append(data["temperature"])
    daily_environment_data[date]["humidity"].append(data["humidity"])
    daily_environment_data[date]["pressure"].append(data["pressure"])
    daily_environment_data[date]["air_quality"].append(data["air_quality"])
    daily_environment_data[date]["noise_level"].append(data["noise_level"])


# Aggregate traffic data
def aggregate_traffic_data(data):
    date = datetime.fromtimestamp(data["timestamp"]).strftime("%Y-%m-%d")
    daily_traffic_data[date]["vehicle_count"].append(data["vehicle_count"])
    daily_traffic_data[date]["average_speed"].append(data["average_speed"])
    daily_traffic_data[date]["traffic_density"].append(data["traffic_density"])


# Calculate daily metrics for environmental data
def calculate_daily_environment_metrics(date):
    temp_data = daily_environment_data[date]["temperature"]
    hum_data = daily_environment_data[date]["humidity"]
    pres_data = daily_environment_data[date]["pressure"]
    air_quality_data = daily_environment_data[date]["air_quality"]
    noise_level_data = daily_environment_data[date]["noise_level"]

    return {
        "date": date,
        "temperature_avg": sum(temp_data) / len(temp_data),
        "temperature_max": max(temp_data),
        "temperature_min": min(temp_data),
        "humidity_avg": sum(hum_data) / len(hum_data),
        "humidity_max": max(hum_data),
        "humidity_min": min(hum_data),
        "pressure_avg": sum(pres_data) / len(pres_data),
        "pressure_max": max(pres_data),
        "pressure_min": min(pres_data),
        "air_quality_avg": sum(air_quality_data) / len(air_quality_data),
        "air_quality_max": max(air_quality_data),
        "air_quality_min": min(air_quality_data),
        "noise_level_avg": sum(noise_level_data) / len(noise_level_data),
        "noise_level_max": max(noise_level_data),
        "noise_level_min": min(noise_level_data),
    }


# Calculate daily metrics for traffic data
def calculate_daily_traffic_metrics(date):
    vehicle_count_data = daily_traffic_data[date]["vehicle_count"]
    average_speed_data = daily_traffic_data[date]["average_speed"]
    traffic_density_data = daily_traffic_data[date]["traffic_density"]

    return {
        "date": date,
        "vehicle_count_total": sum(vehicle_count_data),
        "average_speed_avg": sum(average_speed_data) / len(average_speed_data),
        "average_speed_max": max(average_speed_data),
        "average_speed_min": min(average_speed_data),
        "traffic_density_avg": sum(traffic_density_data) / len(traffic_density_data),
    }


# Consume and process sensor data
for message in consumer:
    data = message.value
    topic = message.topic

    if topic == "environment-data":
        print(f"Consumed Environment Data: {data}")
        aggregate_environment_data(data)

        if datetime.now().second == 0:
            date = datetime.now().strftime("%Y-%m-%d")
            if date in daily_environment_data:
                metrics = calculate_daily_environment_metrics(date)
                print(f"Daily Environment Metrics for {date}: {metrics}")

    elif topic == "traffic-data":
        print(f"Consumed Traffic Data: {data}")
        aggregate_traffic_data(data)

        if datetime.now().second == 0:
            date = datetime.now().strftime("%Y-%m-%d")
            if date in daily_traffic_data:
                metrics = calculate_daily_traffic_metrics(date)
                print(f"Daily Traffic Metrics for {date}: {metrics}")
