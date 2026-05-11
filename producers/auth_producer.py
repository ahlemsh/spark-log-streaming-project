from kafka import KafkaProducer
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: v.encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

hostnames = ['server01', 'server02', 'auth-node1']
users = ['root', 'admin', 'john', 'mary']
processes = ['auth', 'sshd', 'sudo']
ips = ['192.168.1.1', '10.0.0.2', '172.16.0.5', '8.8.8.8']

print("Auth producer démarré — format Syslog...")

while True:
    now = datetime.now().strftime("%Y %b %d %H:%M:%S")
    hostname = random.choice(hostnames)
    process = random.choice(processes)
    pid = random.randint(1000, 9999)
    user = random.choice(users)
    ip = random.choice(ips)
    action = random.choices(
        ['Failed password', 'Accepted password', 'session opened', 'FAILED su'],
        weights=[30, 50, 15, 5]
    )[0]

    log = f"{now} {hostname} {process}[{pid}]: {action} for user {user} from {ip}"

    producer.send('auth-logs', key='auth', value=log)
    print(f"[SYSLOG] {log}")
    time.sleep(0.5)