import sys, time, random
from myqueue_library import MyProducer

broker_url = sys.argv[1]
file_path = sys.argv[2]

logs = []

with open(file_path, 'r') as fin:
    logs = fin.readlines()

topics = set() 

for log in logs:
    topic = log.split()[3]
    if topic not in topics:
        topics.add(topic)

producer = MyProducer(topics = list(topics), broker=broker_url)



for idx, log in enumerate(logs):
    if(idx % 50 == 0):
        print(f'Number of messages sent: {idx}')
        
    time_to_sleep = random.randint(0, 10) / 10
    
    producer.send(topic=log.split()[3], message=log.split()[1])
    time.sleep(time_to_sleep)
