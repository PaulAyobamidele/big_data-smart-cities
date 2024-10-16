from kafka import KafkaProducer
import json
import time
import random

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# Simulate environmental sensor data
def generate_environment_data():
    return {
        "timestamp": time.time(),
        "temperature": round(random.uniform(15, 35), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "pressure": round(random.uniform(980, 1050), 2),
        "air_quality": round(random.uniform(0, 300), 2),  # Air quality index
        "noise_level": round(random.uniform(30, 100), 2),  # Noise level in dB
    }


# Simulate traffic sensor data
def generate_traffic_data():
    return {
        "timestamp": time.time(),
        "vehicle_count": random.randint(50, 200),  # Number of vehicles passing
        "average_speed": round(random.uniform(20, 80), 2),  # Speed in km/h
        "traffic_density": round(
            random.uniform(0.1, 1.0), 2
        ),  # Traffic density as a ratio
    }


# Send data to respective Kafka topics
while True:
    environment_data = generate_environment_data()
    traffic_data = generate_traffic_data()

    producer.send("environment-data", environment_data)
    producer.send("traffic-data", traffic_data)

    print(f"Produced Environment Data: {environment_data}")
    print(f"Produced Traffic Data: {traffic_data}")

    time.sleep(1)  # Simulate data every second
