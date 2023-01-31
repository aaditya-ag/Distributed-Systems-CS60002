from flask import Flask, jsonify, request
from flask_restful import Resource, Api, reqparse
import threading
import time

app = Flask(__name__)
api = Api(app)

# topics = [
#     "hello",
#     "bye"
# ]

# producers = {
#     "hello": [0,1],
#     "bye": [0]
# }

# consumers = {
#     "hello": [[0,0]],
#     "bye": [[0,0], [1,0]]
# }

# logs = {
#     "hello": ["msg1", "msg2"],
#     "bye": ["msg1"]
# }


class Producers:
    def __init__(self):
        self.producers = {}
        self.lock = threading.Semaphore()
        self.count = 0

    def get_topics(self):
        return self.producers.keys()

    def add_producer(self, topic):
        next_producer_id = 0
        self.lock.acquire()
        if(topic not in self.get_topics()):
            self.producers[topic] = []
        next_producer_id = self.count
        self.producers[topic].append(next_producer_id)
        self.count += 1
        self.lock.release()
        return next_producer_id


class Consumers: 
    def __init__(self):
        self.consumers = {}
        self.lock = threading.Semaphore()
        self.count = 0
    
    def get_topics(self):
        return self.consumers.keys()

    def add_consumer(self, topic):
        next_consumer_id = 0
        self.lock.acquire()
        if(topic not in self.get_topics()):
            self.consumers[topic] = []
        next_consumer_id = self.count
        self.consumers[topic].append([next_consumer_id,0])
        self.count += 1
        self.lock.release()
        return next_consumer_id

    
class MasterQueue:
    def __init__(self):
        self.queues = {} # basically, the logs dictionary
        self.locks = {}
        self.global_lock = threading.Semaphore()
    
    def get_topics(self):
        return list(self.queues.keys())

    def add_topic(self, topic):
        self.global_lock.acquire()
        self.queues[topic] = []
        self.locks[topic] = threading.Semaphore()
        self.global_lock.release()

    def add_message(self, topic, message):
        self.locks[topic].acquire()
        self.queues[topic].append(message)
        self.locks[topic].release()
    

m_queue = MasterQueue()
m_producers = Producers()
m_consumers = Consumers()

class Topics(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('name', required = True, help = '"Name" field should be provided in the body')

    def get(self):
        return {
            "status": "Success",
            "topics": m_queue.get_topics()
        }

    def post(self):
        args = Topics.parser.parse_args()
        if(args["name"] not in m_queue.get_topics()): 
            m_queue.add_topic(args["name"])
            return {
                "status": "Success",
                "message": "Topic \'" + request.get_json()["name"] + "\' created."
            }
        else: 
            return {
                "status": "Failure",
                "message": "Topic \'" + request.get_json()["name"] + "\' already exists."
            }

class ConsumerRegister(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = '\"topic\" field should be provided in the body')

    def post(self):
        args = ConsumerRegister.parser.parse_args()

        ## DEBUG ##
        print(args["topic"])
        ###########
        topics = m_queue.get_topics()
        if args["topic"] not in topics :
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        else:
            next_consumer_id = m_consumers.add_consumer(args["topic"])
        
            ## DEBUG ##
            print(m_consumers.consumers)
            ###########

            return {
                "status": "Success",
                "consumer_id": next_consumer_id,
                "message": "Subscribed to topic '" + request.get_json()["topic"] + "'."
            }

class ProducerRegister(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = '"topic" field should be provided in the body')

    def post(self):
        args = ProducerRegister.parser.parse_args()

        ## DEBUG ##
        print(args["topic"])
        ###########

        if(args["topic"] not in m_queue.get_topics()):
            m_queue.add_topic(args["topic"])

        next_producer_id = m_producers.add_producer(args["topic"])
        
        ## DEBUG ##
        print(m_producers.producers)
        ###########

        return {
            "status": "Success",
            "producer_id": next_producer_id,
            "message": "Subscribed to topic '" + request.get_json()["topic"] + "'."
        }

class Enqueue(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('producer_id', required = True, help = 'producer ID of the client')
    parser.add_argument('message', required = True, help = 'message to be added to the queue')

    def post(self):
        args = Enqueue.parser.parse_args()
        
        if args["topic"] not in m_queue.get_topics():
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        if int(args["producer_id"]) not in m_producers.producers[args["topic"]]:
            return {
                "status": "Failure",
                "message": "Producer ID '" + request.get_json()["producer_id"] + "' is not registered for the given topic."
            }
        m_queue.add_message(args["topic"], args["message"])
        # logs[args["topic"]].append(args["message"])
        return {
            "status": "Success",
            "message": "Message '" + request.get_json()["message"] + "' added for the topic."
        }

class Dequeue(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('consumer_id', required = True, help = 'consumer ID of the client')

    def get(self):
        args = Dequeue.parser.parse_args()
        
        if args["topic"] not in m_queue.get_topics():
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        cons_id = [i[0] for i in m_consumers.consumers[args["topic"]]]
        if int(args["consumer_id"]) not in cons_id:
            return {
                "status": "Failure",
                "message": "Consumer ID '" + request.get_json()["consumer_id"] + "' is not registered for the given topic."
            }
        ind = m_consumers.consumers[args["topic"]][cons_id.index(int(args["consumer_id"]))][1]
        if ind >= len( m_queue.queues[args["topic"]] ):
             return {
                "status": "Failure",
                "message": "No new updates/messages for the given topic."
            }
        msg = m_queue.queues[args["topic"]][ind]
        return {
            "status": "Success",
            "message": msg
        }
    
class Size(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('consumer_id', required = True, help = 'consumer ID of the client')

    def get(self):
        args = Size.parser.parse_args()
        
        if args["topic"] not in m_queue.get_topics():
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        cons_id = [i[0] for i in m_consumers.consumers[args["topic"]]]
        if int(args["consumer_id"]) not in cons_id:
            return {
                "status": "Failure",
                "message": "Consumer ID '" + request.get_json()["consumer_id"] + "' is not registered for the given topic."
            }
        ind = m_consumers.consumers[args["topic"]][cons_id.index(int(args["consumer_id"]))][1]
        if ind >= len(m_queue.queues[args["topic"]]):
             return {
                "status": "Success",
                "message": "No new updates/messages for the given topic."
            }
        size = len(m_queue.queues[args["topic"]]) - ind
        return {
            "status": "Success",
            "size": size
        }

api.add_resource(Topics, '/topics')
api.add_resource(ConsumerRegister, '/consumer/register')
api.add_resource(Dequeue, '/consumer/consume')
api.add_resource(ProducerRegister, '/producer/register')
api.add_resource(Enqueue, '/producer/produce')
api.add_resource(Size, '/size')


if __name__ == '__main__':
    app.run(port=5000)
