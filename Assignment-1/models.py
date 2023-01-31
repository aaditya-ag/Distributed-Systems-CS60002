from app import db

class TopicsModel(db.Model):

    # We always need an id
    id = db.Column(db.Integer, primary_key=True)

    # A Topic must have a name
    name = db.Column(db.String, nullable=False, unique=True)


    def __init__(self, name):
        self.name = name
    
    def as_dict(self):
        return  {
            "id": self.id,
            "name": self.name
        }

class ProducerModel(db.Model):
    # A Producer must have an id
    producer_id = db.Column(db.Integer, primary_key=True)

    # We need a topic for which this producer is registering
    topic_id = db.Column(db.Integer, db.ForeignKey(TopicsModel.id))

    
    def __init__(self, topic_id):
        self.topic_id = topic_id
    
    def as_dict(self):
        return  {
            "producer_id": self.producer_id,
            "topic_id": self.topic_id
        }

class ConsumerModel(db.Model):
    # A Consumer must have an id
    consumer_id = db.Column(db.Integer, primary_key=True)

    # We need a topic for which the consumer registers 
    topic_id = db.Column(db.Integer, db.ForeignKey(TopicsModel.id))

    # Maintain an index upto which the consumer has read the messages
    idx_read_upto = db.Column(db.Integer)


    def __init__(self, topic_id):
        self.topic_id = topic_id
        # initially the consumer has not read any messages, so set the index value to 0
        self.idx_read_upto = 0
    
    def as_dict(self):
        return  {
            "consumer_id": self.consumer_id,
            "topic_id": self.topic_id,
            "idx_read_upto": self.idx_read_upto
        }


class QueueModel(db.Model):

    # We always need an id for each entry in the db
    id = db.Column(db.Integer, primary_key=True)

    # We need a reference topic id as ForeignKey for each Log entry
    topic_id = db.Column(db.Integer, db.ForeignKey(TopicsModel.id))

    # We need to store the content of the log message
    log_message = db.Column(db.String, nullable=False)

    # We need an index of the message to retrieve it in future from the database
    message_index = db.Column(db.Integer, index=True)


    def __init__(self, topic_id, message, message_index):
        self.topic_id = topic_id
        self.log_message = message
        self.message_index = message_index

    def as_dict(self):
        return  {
            "id": self.id,
            "topic_id": self.topic_id,
            "message": self.log_message,
            "message_index":self.message_index
        }