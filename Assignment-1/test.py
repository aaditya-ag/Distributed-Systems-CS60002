from myqueue import MyProducer, MyConsumer

def produce():
    producer = MyProducer(
    topics=['topic1', 'topic2'],
    broker='http://localhost:5000')
    try:
        # Produce message and wait for it to be sent
        producer.send("topic1", "DEBUG: This is a log message!")
    except Exception as e:
        # Wait for all messages to be delivered or expire
        print(f"Error Occurred => {e}")

# Consumer client:
def consume():
    consumer = MyConsumer(
    topics=[ 'topic1', 'topic2'],
    broker='http://localhost:5000')
    try:
        # Consume messages
        while consumer.has_next('topic1'):
        # get_next() can be implemented as a generator
            print("consumed: ", consumer.get_next('topic1'))
    except Exception as e:
        # Wait for all messages to be delivered or expire
        print(f"Error Occurred => {e}")


produce()
consume()