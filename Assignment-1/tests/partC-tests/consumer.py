import sys
from myqueue_library import MyConsumer

broker_url = sys.argv[1]
out_file_base = sys.argv[2]
topics = sys.argv[3:]

consumer = MyConsumer(topics = topics, broker = broker_url)

while(True):
    for topic in topics:
        if(consumer.has_next(topic)):
            message = consumer.get_next(topic)
            print("read message:", message)
            with open(out_file_base + "_" + topic + ".txt", "a") as fout:
                fout.write(message)
                fout.write("\n")

