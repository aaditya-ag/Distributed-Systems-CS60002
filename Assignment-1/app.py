from flask import Flask, jsonify, request
from flask_restful import Resource, Api, reqparse
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:admin@localhost:5432/distributed_queue"
db = SQLAlchemy(app)
migrate = Migrate(app, db)

api = Api(app)

from models import (
    TopicsModel, 
    ProducerModel, 
    ConsumerModel,
    QueueModel
)

class Topics(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('name', required = True, help = '"Name" field should be provided in the body')

    def get(self):
        topics = TopicsModel.query.all()
        topics = [topic.name for topic in topics]
        return {
            "status": "Success",
            "topics": topics
        }, 200

    def post(self):
        args = Topics.parser.parse_args()

        if TopicsModel.query.filter_by(name=args["name"]).first() is not None:
            return {
                "status": "Failure",
                "message": "Topic \'" + request.get_json()["name"] + "\' already exists."
            }, 400
        else:
            topic = TopicsModel(name = args["name"])
            db.session.add(topic)
            db.session.commit()
            return {
                "status": "Success",
                "message": "Topic \'" + topic.name + "\' created."
            }, 200


class ConsumerRegister(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = '\"topic\" field should be provided in the body')

    def post(self):
        args = ConsumerRegister.parser.parse_args()

        ## DEBUG ##
        print(args["topic"])
        ###########

        topic = TopicsModel.query.filter_by(name=args["topic"]).first()

        # If topic doesn't exist then return error.
        if topic is None:
            return {
                "status": "Failure",
                "message": "Topic '" + args["topic"] + "' doesn't exist."
            }, 400
        
        topic_id = topic.id
        consumer = ConsumerModel(topic_id=topic_id)
        db.session.add(consumer)
        db.session.flush()
        db.session.commit()

        return {
            "status": "Success",
            "consumer_id": consumer.consumer_id,
            "message": "Subscribed to topic '" + topic.name + "'."
        }, 200

class ProducerRegister(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = '"topic" field should be provided in the body')
    
    def post(self):
        args = ProducerRegister.parser.parse_args()
        
        ## DEBUG ##
        print(args["topic"])
        ###########

        topic = TopicsModel.query.filter_by(name=args["topic"]).first()

        # If topic doesn't exist then create one.
        if topic is None:
            topic = TopicsModel(name = args["topic"])
            db.session.add(topic)
            db.session.flush()
            db.session.commit()
        
        topic_id = topic.id
        producer = ProducerModel(topic_id=topic_id)
        db.session.add(producer)
        db.session.flush()
        db.session.commit()

        return {
            "status": "Success",
            "producer_id": producer.producer_id,
            "message": "Subscribed to topic '" + request.get_json()["topic"] + "'."
        }, 200


class Enqueue(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('producer_id', required = True, help = 'producer ID of the client')
    parser.add_argument('message', required = True, help = 'message to be added to the queue')

    def post(self):
        args = Enqueue.parser.parse_args()
        
        topic = TopicsModel.query.filter_by(name=args["topic"]).first()
        
        # If topic doesn't exist then return error.
        if topic is None:
            return {
                "status": "Failure",
                "message": f"Topic {args['topic']} doesn't exist."
            }, 400

        producer = ProducerModel.query.filter_by(producer_id = args["producer_id"]).first()
       
        # If producer does not exist then return error
        if producer is None:
            return {
                "status": "Failure",
                "message": f"Producer with id = {args['producer_id']} doesn't exist."
            }, 400

        # if producer's topic doesn't match with the topic sent in argument, then return error  
        if producer.topic_id != topic.id:
             return {
                "status": "Failure",
                "message": f"Producer with id = {args['producer_id']} doesn't have access to the topic {topic.name}"
            }, 400

        # Get the next message index from the database in this queue
        msg_index = QueueModel.query.filter_by(topic_id = topic.id).count()
        
        # Create the log message
        log_message = QueueModel(topic_id=topic.id, message=args["message"], message_index=msg_index)
        db.session.add(log_message)
        db.session.commit()

        return {
            "status": "Success",
            "message": f"Message `{log_message.log_message}` added for the topic."
        }, 200


class Dequeue(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('consumer_id', required = True, help = 'consumer ID of the client')

    def get(self):
        args = Dequeue.parser.parse_args()

        topic = TopicsModel.query.filter_by(name=args["topic"]).first()
        
        # If topic doesn't exist then return error.
        if topic is None:
            return {
                "status": "Failure",
                "message": f"Topic {args['topic']} doesn't exist."
            }, 400

        consumer = ConsumerModel.query.filter_by(consumer_id = args["consumer_id"]).first()

        # If Consumer does not exist then return error
        if consumer is None:
            return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't exist."
            }, 400

        # if consumer's topic doesn't match with the topic sent in argument, then return error  
        if consumer.topic_id != topic.id:
             return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't have access to the topic {topic.name}"
            }, 400

        num_log_messages = QueueModel.query.filter_by(topic_id = topic.id).count()
        
        # If no new messages to read, then return error
        if consumer.idx_read_upto >= num_log_messages:
            return {
                "status": "Failure",
                "message": "No new updates/messages for the given topic."
            }, 400
        
        log_msg_entry = QueueModel.query.filter_by(topic_id = topic.id, message_index=consumer.idx_read_upto).first()
        consumer.idx_read_upto += 1
        db.session.commit()
        return {
            "status": "Success",
            "message": f"Message `{log_msg_entry.log_message}` retrieved for the topic."
        }, 200
        
    
class Size(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('topic', required = True, help = 'topic name for the message to be added')
    parser.add_argument('consumer_id', required = True, help = 'consumer ID of the client')

    def get(self):
        args = Size.parser.parse_args()

        topic = TopicsModel.query.filter_by(name=args["topic"]).first()
        
        # If topic doesn't exist then return error.
        if topic is None:
            return {
                "status": "Failure",
                "message": f"Topic {args['topic']} doesn't exist."
            }, 400

        consumer = ConsumerModel.query.filter_by(consumer_id = args["consumer_id"]).first()

        # If Consumer does not exist then return error
        if consumer is None:
            return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't exist."
            }, 400

        # if consumer's topic doesn't match with the topic sent in argument, then return error  
        if consumer.topic_id != topic.id:
             return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't have access to the topic {topic.name}"
            }, 400

        num_log_messages = QueueModel.query.filter_by(topic_id = topic.id).count() - consumer.idx_read_upto
        return {
            "status": "Success",
            "size": num_log_messages
        }, 200


api.add_resource(Topics, '/topics')
api.add_resource(ConsumerRegister, '/consumer/register')
api.add_resource(Dequeue, '/consumer/consume')
api.add_resource(ProducerRegister, '/producer/register')
api.add_resource(Enqueue, '/producer/produce')
api.add_resource(Size, '/size')


if __name__ == '__main__':
    app.run(port=5000)
