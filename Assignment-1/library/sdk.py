import requests

class message_queue():
    def __init__(self,url) -> None:
        self.url = url

    def create_topic(self,name):
        parameters ={
            'name' : name
        }
        r = requests.post(self.url+"/topics", data = parameters)
        jsonobj = r.json()
        if r.ok :
            if (jsonobj.get("status") == "Success") :
                return jsonobj.get("message")
            elif (jsonobj.get("status") == "Failure") :
                return jsonobj.get("message")
        else:
            print("HTTP error")
            return None

    def list_topics(self):
        r = requests.get(self.url+"/topics")
        jsonobj = r.json()
        
        if r.ok :
            if (jsonobj.get("status") == "Success") :
                return jsonobj.get("topics")
            elif (jsonobj.get("status") == "Failure") :
                return jsonobj.get("message")
        else:
            print("HTTP error")
            return None

    def register_consumer(self,topic):
        parameters ={
            'topic' : topic
        }
        r = requests.post(self.url+"/consumer/register", data = parameters)
        jsonobj = r.json()

        if r.ok :
            if (jsonobj.get("status") == "Success") :
                return jsonobj.get("consumer_id")
            elif (jsonobj.get("status") == "Failure") :
                return jsonobj.get("message")
        else:
            print("HTTP error")
            return None

    def register_producer(self, topic):
        parameters = {
            'topic' : topic
        }
        r = requests.post(self.url+"/producer/register", data = parameters)
        jsonobj = r.json()

        if r.ok :
            if (jsonobj.get("status") == "Success") :
                return jsonobj.get("producer_id")
            elif (jsonobj.get("status") == "Failure") :
                return jsonobj.get("message")
        else:
            print("HTTP error")
            return None

    class Producer:
        def enque(self,topic,producer_id,message):
            parameters = {
                'topic' : topic,
                'producer_id' : producer_id,
                'message' : message
            }
            r = requests.post(self.url+"/producer/produce", data = parameters)
            jsonobj = r.json()
            if r.ok :
                if (jsonobj.get("status") == "Success") :
                    return None
                elif (jsonobj.get("status") == "Failure") :
                    return jsonobj.get("message")
            else:
                print("HTTP error")
                return None

    class Consumer:
        def enque(self,topic,consumerer_id):
            parameters = {
                'topic' : topic,
                'consumerer_id' : consumerer_id
            }
            r = requests.get(self.url+"/consumer/consume", params = parameters)
            jsonobj = r.json()
            if r.ok :
                if (jsonobj.get("status") == "Success") :
                    return jsonobj.get("message")
                elif (jsonobj.get("status") == "Failure") :
                    return jsonobj.get("message")
            else:
                print("HTTP error")
                return None

    def size(self,topic,consumer_id):
        parameters={
            'topic' : topic,
            'consumer_id' : consumer_id
        }
        r = requests.get(self.url+"/size", params = parameters)
        jsonobj = r.json()
        
        if r.ok :
            if (jsonobj.get("status") == "Success") :
                return jsonobj.get("size")
            elif (jsonobj.get("status") == "Failure") :
                return jsonobj.get("message")
        else:
            print("HTTP error")
            return None

    

