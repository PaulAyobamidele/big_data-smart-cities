import time

producer = KafkaProducer(bootstrap_servers="localhost:9092")

for i in range(10):
    message = f"Message number {i}"
    producer.send("TestTopic", value=message.encode("utf-8"))
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
producer.close()
