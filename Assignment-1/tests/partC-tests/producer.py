import sys, time, random
from ...myqueue import MyProducer

file_path = sys.argv[2]
broker_url = sys.argv[1]

logs = []

with open(file_path, 'r') as fin:
    logs = fin.readlines()

topics = set() 

for log in logs[:2]:
    topic = log.split()[3]
    if topic not in topics:
        topics.add(topic)

producer = MyProducer(topics = topics, broker=broker_url)

for log in logs[:10]:
    time_to_sleep = random.randint(0, 10) / 10
    
    # producer.send(topic=log.split()[3], message=log.split()[1])
    time.sleep(time_to_sleep)
