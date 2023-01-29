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
    # A Topic must have a name
    producer_id = db.Column(db.Integer, primary_key=True)

    # We always need an id
    topic_id = db.Column(db.Integer, db.ForeignKey(TopicsModel.id))

    

    def __init__(self, topic_id):
        self.topic_id = topic_id
    
    def as_dict(self):
        return  {
            "producer_id": self.producer_id,
            "topic_id": self.topic_id
        }

class ConsumerModel(db.Model):
    # A Topic must have a name
    consumer_id = db.Column(db.Integer, primary_key=True)

    # We always need an id
    topic_id = db.Column(db.Integer, db.ForeignKey(TopicsModel.id))

    idx_read_upto = db.Column(db.Integer)

    

    def __init__(self, topic_id):
        self.topic_id = topic_id
        self.idx_read_upto = 0
    
    def as_dict(self):
        return  {
            "consumer_id": self.consumer_id,
            "topic_id": self.topic_id,
            "idx_read_upto": self.idx_read_upto
        }