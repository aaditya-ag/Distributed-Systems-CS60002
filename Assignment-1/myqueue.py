import requests

def get(url, payload):
    response = requests.get(url, json=payload)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return {
            "status": "Failure",
            "message": "Request failed with status code: " + str(response.status_code)
        }

def post(url, payload):
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return {
            "status": "Failure",
            "message": "Request failed with status code: " + str(response.status_code)
        }

class MyProducer():
    def __init__(self, topics, broker):
        self.topics = {}
        self.broker = broker

        lst = [self.broker]
        lst.extend(['producer', 'register'])
        url = '/'.join(lst)

        for topic in topics:
            payload = {"topic": topic}
            result = post(url, payload)
            if result['status'] == "Success":
                self.topics[topic] = result['producer_id']
            else:
                raise Exception(result['message'])

    def send(self, topic, message):
        lst = [self.broker]
        lst.extend(['producer', 'produce'])
        url = '/'.join(lst)

        payload = {
            "topic": topic,
            "producer_id": self.topics[topic],
            "message": message
        }
        result = post(url, payload)
        if result['status'] != "Success":
            raise Exception(result['message'])

class MyConsumer():
    def __init__(self, topics, broker):
        self.topics = {}
        self.broker = broker

        lst = [self.broker]
        lst.extend(['consumer', 'register'])
        url = '/'.join(lst)

        for topic in topics:
            payload = {"topic": topic}
            result = post(url, payload)
            if result['status'] == "Success":
                self.topics[topic] = result['consumer_id']
            else:
                raise Exception(result['message'])
    
    def has_next(self, topic):
        lst = [self.broker]
        lst.append('size')
        url = '/'.join(lst)

        payload = {
            "topic": topic,
            "consumer_id": self.topics[topic],
        }
        result = get(url, payload)
        if result['status'] != "Success":
            raise Exception(result['message'])
        elif result['size'] == 0:
            return False
        return True

    def get_next(self, topic):
        lst = [self.broker]
        lst.extend(['consumer', 'consume'])
        url = '/'.join(lst)

        payload = {
            "topic": topic,
            "consumer_id": self.topics[topic],
        }
        result = get(url, payload)
        if result['status'] != "Success":
            raise Exception(result['message'])
        return result['message']
    
    def size(self, topic):
        lst = [self.broker]
        lst.append('size')
        url = '/'.join(lst)

        payload = {
            "topic": topic,
            "consumer_id": self.topics[topic],
        }
        result = get(url, payload)
        if result['status'] != "Success":
            raise Exception(result['message'])
        return result['size']
