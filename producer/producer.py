import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    data = {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 60), 2),
        "timestamp": int(time.time())
    }

    producer.send("sensor-data", value=data)
    print("Enviado:", data)
    time.sleep(2)