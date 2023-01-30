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
    LogsModel
)

# This is a basic app.py, doesn't have logic implemented in it


# Dictionary: indexed by topic name
# Each entry is a list of tuples (a,b) 
# where a = topic-specific consumer id
# and b = next read index for the consumer 

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
        topics = TopicsModel.query.all()
        topics = [topic.as_dict() for topic in topics]
        return {
            "status": "Success",
            "topics": topics
        }

    def post(self):
        args = Topics.parser.parse_args()

        if TopicsModel.query.filter_by(name=args["name"]).first() is not None:
            return {
                "status": "Failure",
                "message": "Topic \'" + request.get_json()["name"] + "\' already exists."
            }
        else:
            topic = TopicsModel(name = args["name"])
            db.session.add(topic)
            db.session.commit()
            return {
                "status": "Success",
                "message": "Topic \'" + topic.name + "\' created."
            }

        # if(args["name"] not in topics): 
        #     topics.append(args["name"])
        #     return {
        #         "status": "Success",
        #         "message": "Topic \'" + request.get_json()["name"] + "\' created."
        #     }
        # else: 
        #     return {
        #         "status": "Failure",
        #         "message": "Topic \'" + request.get_json()["name"] + "\' already exists."
        #     }

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
            }
        
        topic_id = topic.id
        consumer = ConsumerModel(topic_id=topic_id)
        db.session.add(consumer)
        db.session.flush()
        db.session.commit()

        return {
            "status": "Success",
            "consumer_id": consumer.consumer_id,
            "message": "Subscribed to topic '" + topic.name + "'."
        }

        # if args["topic"] not in topics:
        #     return {
        #         "status": "Failure",
        #         "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        #     }
        # else:
        #     next_consumer_id = 0

        #     if(args["topic"] not in consumers.keys()):
        #         consumers[args["topic"]] = []
        #         consumers[args["topic"]].append([next_consumer_id,0])
        #     else: 
        #         for _consumer in consumers[args["topic"]]:
        #             next_consumer_id = max(next_consumer_id, _consumer[0])
        #         next_consumer_id += 1
        #         consumers[args["topic"]].append([next_consumer_id,0])

        #     ## DEBUG ##
        #     print(consumers)
        #     ###########

        #     return {
        #         "status": "Success",
        #         "consumer_id": next_consumer_id,
        #         "message": "Subscribed to topic '" + request.get_json()["topic"] + "'."
        #     }

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
        }



        # next_producer_id = 0
        # if args["topic"] not in topics:
        #     # if topic doesnt exist, add the topic
        #     topics.append(args["topic"])
        #     # since topic is newly added, create a new topic entry in the producers dict
        #     producers[args["topic"]] = [next_producer_id]
        # else:
        #     # topic exist but no one has registered to it as producer
        #     if(args["topic"] not in producers.keys()):
        #         producers[args["topic"]] = [next_producer_id]
        #     # already existing entries for producers of the topic
        #     else:
        #         for _producer in producers[args["topic"]]:
        #             next_producer_id = max(_producer, next_producer_id)
        #         next_producer_id += 1
        #         producers[args["topic"]].append(next_producer_id)

        # ## DEBUG ##
        # print(producers)
        # ###########

        # return {
        #     "status": "Success",
        #     "producer_id": next_producer_id,
        #     "message": "Subscribed to topic '" + request.get_json()["topic"] + "'."
        # }

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
            }

        producer = ProducerModel.query.filter_by(producer_id = args["producer_id"]).first()
       
        # If producer does not exist then return error
        if producer is None:
            return {
                "status": "Failure",
                "message": f"Producer with id = {args['producer_id']} doesn't exist."
            }

        # if producer's topic doesn't match with the topic sent in argument, then return error  
        if producer.topic_id != topic.id:
             return {
                "status": "Failure",
                "message": f"Producer with id = {args['producer_id']} doesn't have access to the topic {topic.name}"
            }

        # Get the next message index from the database in this queue
        msg_index = LogsModel.query.filter_by(topic_id = topic.id).count()
        
        # Create the log message
        log_message = LogsModel(topic_id=topic.id, message=args["message"], message_index=msg_index)
        db.session.add(log_message)
        db.session.commit()

        return {
            "status": "Success",
            "message": f"Message `{log_message.message}` added for the topic."
        }

        # if args["topic"] not in topics:
        #     return {
        #         "status": "Failure",
        #         "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        #     }
        # if int(args["producer_id"]) not in producers[args["topic"]]:
        #     return {
        #         "status": "Failure",
        #         "message": "Producer ID '" + request.get_json()["producer_id"] + "' is not registered for the given topic."
        #     }
        # logs[args["topic"]].append(args["message"])
        # return {
        #     "status": "Success",
        #     "message": "Message '" + request.get_json()["message"] + "' added for the topic."
        # }

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
            }

        consumer = ConsumerModel.query.filter_by(consumer_id = args["consumer_id"]).first()

        # If Consumer does not exist then return error
        if consumer is None:
            return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't exist."
            }

        # if consumer's topic doesn't match with the topic sent in argument, then return error  
        if consumer.topic_id != topic.id:
             return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't have access to the topic {topic.name}"
            }

        num_log_messages = LogsModel.query.filter_by(topic_id = topic.id).count()
        
        # If no new messages to read, then return error
        if consumer.idx_read_upto >= num_log_messages:
            return {
                "status": "Failure",
                "message": "No new updates/messages for the given topic."
            }
        
        log_msg_entry = LogsModel.query.filter_by(topic_id = topic.id, message_index=consumer.idx_read_upto).first()
        consumer.idx_read_upto += 1
        db.session.commit()
        return {
            "status": "Success",
            "message": f"Message `{log_msg_entry.message}` retrieved for the topic."
        }
        
        # if args["topic"] not in topics:
        #     return {
        #         "status": "Failure",
        #         "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        #     }
        # cons_id = [i[0] for i in consumers[args["topic"]]]
        # if int(args["consumer_id"]) not in cons_id:
        #     return {
        #         "status": "Failure",
        #         "message": "Consumer ID '" + request.get_json()["consumer_id"] + "' is not registered for the given topic."
        #     }
        # ind = consumers[args["topic"]][cons_id.index(int(args["consumer_id"]))][1]
        # if ind >= len(logs[args["topic"]]):
        #      return {
        #         "status": "Failure",
        #         "message": "No new updates/messages for the given topic."
        #     }
        # msg = logs[args["topic"]][ind]
        # return {
        #     "status": "Success",
        #     "message": "Message " + msg + " retrieved for the topic."
        # }
    
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
            }

        consumer = ConsumerModel.query.filter_by(consumer_id = args["consumer_id"]).first()

        # If Consumer does not exist then return error
        if consumer is None:
            return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't exist."
            }

        # if consumer's topic doesn't match with the topic sent in argument, then return error  
        if consumer.topic_id != topic.id:
             return {
                "status": "Failure",
                "message": f"Consumer with id = {args['consumer_id']} doesn't have access to the topic {topic.name}"
            }

        num_log_messages = LogsModel.query.filter_by(topic_id = topic.id).count() - consumer.idx_read_upto
        return {
            "status": "Success",
            "size": num_log_messages
        }

        # if args["topic"] not in topics:
        #     return {
        #         "status": "Failure",
        #         "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        #     }
        # cons_id = [i[0] for i in consumers[args["topic"]]]
        # if int(args["consumer_id"]) not in cons_id:
        #     return {
        #         "status": "Failure",
        #         "message": "Consumer ID '" + request.get_json()["consumer_id"] + "' is not registered for the given topic."
        #     }
        # ind = consumers[args["topic"]][cons_id.index(int(args["consumer_id"]))][1]
        # if ind >= len(logs[args["topic"]]):
        #      return {
        #         "status": "Success",
        #         "message": "No new updates/messages for the given topic."
        #     }
        # size = len(logs[args["topic"]]) - ind
        # return {
        #     "status": "Success",
        #     "message": str(size) + " updates/messages for the given topic."
        # }

api.add_resource(Topics, '/topics')
api.add_resource(ConsumerRegister, '/consumer/register')
api.add_resource(Dequeue, '/consumer/consume')
api.add_resource(ProducerRegister, '/producer/register')
api.add_resource(Enqueue, '/producer/produce')
api.add_resource(Size, '/size')


if __name__ == '__main__':
    app.run(port=5000)
