from kafka import KafkaProducer
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: v.encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

ips = ['192.168.1.1', '10.0.0.2', '172.16.0.5', '8.8.8.8']
methods = ['GET', 'POST', 'PUT', 'DELETE']
endpoints = ['/api/users', '/api/orders', '/api/products', '/health']
status_codes = [200, 200, 200, 301, 404, 500]
users = ['frank', 'john', 'mary', '-']

print("Microservices producer démarré — format Apache Common Log...")

while True:
    ip = random.choice(ips)
    user = random.choice(users)
    now = datetime.now().strftime("%d/%b/%Y:%H:%M:%S +0000")
    method = random.choice(methods)
    endpoint = random.choice(endpoints)
    status = random.choice(status_codes)
    response_time = random.randint(10, 2000)  

    log = f'{ip} - {user} [{now}] "{method} {endpoint} HTTP/1.1" {status} {response_time}'

    producer.send('microservices-logs', key='microservices', value=log)
    print(f"[APACHE] {log}")
    time.sleep(0.5)