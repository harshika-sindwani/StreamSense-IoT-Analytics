import random
import time
from datetime import datetime

def get_data():
    return {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    while True:
        print(get_data())
        time.sleep(1)