from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

endpoints = ['/api/users', '/api/login', '/api/products', '/health']
ips = ['192.168.1.1', '10.0.0.2', '172.16.0.5', '8.8.8.8']
user_agents = ['Mozilla/5.0', 'Chrome/91.0', 'Safari/537.36']
levels = ['INFO', 'WARN', 'ERROR']

print("Web producer démarré — format JSON...")

while True:
    level = random.choices(levels, weights=[70, 20, 10])[0]
    log = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "source": "web",
        "message": f"Request to {random.choice(endpoints)}",
        "ip": random.choice(ips),
        "user_agent": random.choice(user_agents),
        "response_time": random.randint(10, 2000),
        "status_code": random.choice([200, 200, 200, 404, 500])
    }
    producer.send('web-logs', key='web', value=log)
    print(f"[{log['level']}] {log['message']} - {log['ip']}")
    time.sleep(0.5)