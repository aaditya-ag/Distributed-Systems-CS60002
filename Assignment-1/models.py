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