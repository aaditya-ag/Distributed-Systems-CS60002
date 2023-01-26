from flask import Flask, jsonify, request
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)


# This is a basic app.py, doesn't have logic implemented in it

topics = [
    "hello",
    "bye"
]

producers = {
    "hello": [0,1],
    "bye": [0]
}

consumers = {
    "hello": [[0,0]],
    "bye": [[0,0], [1,0]]
}

logs = {
    "hello": ["msg1", "msg2"],
    "bye": ["msg1"]
}

class Topics(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('name', required = True, help = '"Name" field should be provided in the body')

    def get(self):
        return topics
    
    def post(self):
        args = Topics.parser.parse_args()
        topics.append(args["name"])
        return {
            "status": "Success",
            "message": "Topic '" + args["name"] + "' created."
        }

class ConsumerRegister(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = '"topic" field should be provided in the body')

    def post(self):
        args = ConsumerRegister.parser.parse_args()
        print(args["topic"])
        if args["topic"] not in topics:
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        return {
            "status": "Success",
            "message": "Topic '" + request.get_json()["topic"] + "' created."
        }

class ProducerRegister(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = '"topic" field should be provided in the body')

    def post(self):
        args = ProducerRegister.parser.parse_args()
        print(args["topic"])
        if args["topic"] not in topics:
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        return {
            "status": "Success",
            "message": "Topic '" + request.get_json()["topic"] + "' created."
        }

class Enqueue(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('producer_id', required = True, help = 'producer ID of the client')
    parser.add_argument('message', required = True, help = 'message to be added to the queue')

    def post(self):
        args = Enqueue.parser.parse_args()
        
        if args["topic"] not in topics:
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        if int(args["producer_id"]) not in producers[args["topic"]]:
            return {
                "status": "Failure",
                "message": "Producer ID '" + request.get_json()["producer_id"] + "' is not registered for the given topic."
            }
        logs[args["topic"]].append(args["message"])
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
        
        if args["topic"] not in topics:
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        cons_id = [i[0] for i in consumers[args["topic"]]]
        if int(args["consumer_id"]) not in cons_id:
            return {
                "status": "Failure",
                "message": "Consumer ID '" + request.get_json()["consumer_id"] + "' is not registered for the given topic."
            }
        ind = consumers[args["topic"]][cons_id.index(int(args["consumer_id"]))][1]
        if ind >= len(logs[args["topic"]]):
             return {
                "status": "Failure",
                "message": "No new updates/messages for the given topic."
            }
        msg = logs[args["topic"]][ind]
        return {
            "status": "Success",
            "message": "Message " + msg + " retrieved for the topic."
        }
    
class Size(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('consumer_id', required = True, help = 'consumer ID of the client')

    def get(self):
        args = Size.parser.parse_args()
        
        if args["topic"] not in topics:
            return {
                "status": "Failure",
                "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
            }
        cons_id = [i[0] for i in consumers[args["topic"]]]
        if int(args["consumer_id"]) not in cons_id:
            return {
                "status": "Failure",
                "message": "Consumer ID '" + request.get_json()["consumer_id"] + "' is not registered for the given topic."
            }
        ind = consumers[args["topic"]][cons_id.index(int(args["consumer_id"]))][1]
        if ind >= len(logs[args["topic"]]):
             return {
                "status": "Success",
                "message": "No new updates/messages for the given topic."
            }
        size = len(logs[args["topic"]]) - ind
        return {
            "status": "Success",
            "message": str(size) + " updates/messages for the given topic."
        }

api.add_resource(Topics, '/topics')
api.add_resource(ConsumerRegister, '/consumer/register')
api.add_resource(Dequeue, '/consumer/consume')
api.add_resource(ProducerRegister, '/producer/register')
api.add_resource(Enqueue, '/producer/produce')
api.add_resource(Size, '/size')


if __name__ == '__main__':
    app.run(port=5000)
