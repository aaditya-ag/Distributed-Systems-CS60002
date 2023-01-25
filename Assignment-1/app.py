from flask import Flask, jsonify, request
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)


# This is a basic app.py, doesn't have logic implemented in it

topics = [
    "hello",
    "bye"
]


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

api.add_resource(Topics, '/topics')
api.add_resource(ConsumerRegister, '/consumer/register')
api.add_resource(ProducerRegister, '/producer/register')


if __name__ == '__main__':
    app.run(port=5000)