import requests
import random

base_url = 'http://127.0.0.1:5000/'

def test_get_topics():
    print(50*"-")
    print(":: Testing Get Topics ::")
    print(50*"-")

    resp = requests.get(base_url+"topics")
    # assert(resp.status_code == 200)
    data = resp.json()
    print(data)

    print(50*"-")

def test_create_topic(num_iterations = 1, topic_name = "test_topic"):
    print(50*"-")
    print(":: Testing Create Topic ::")
    print(50*"-")

    for idx in range(1, num_iterations+1):
        data = {
            "name": topic_name + "_" + str(idx)
        }
        resp = requests.post(url=base_url+"topics",json=data)
        # assert(resp.status_code == 200)

    print(50*"-")

def test_register_consumer(topic_name):
    print(50*"-")
    print(":: Testing Register Consumer ::")
    print(50*"-")

    data = {
        "topic": topic_name
    }
    resp = requests.post(url=base_url+"consumer/register", json=data)
    resp_data = resp.json()
    # assert(resp.status_code == 200)
    assert(resp_data["status"] == "Success")
    consumer_id = resp_data["consumer_id"]
    print(f"Consumer id = {consumer_id} for topic '{topic_name}'")

    print(50*"-")
    return consumer_id


if __name__ == '__main__':
    test_create_topic(num_iterations = 10)
    test_get_topics()

    consumer_ids = {}
    for topic_num in range(1, 11):
        topic_name = "test_topic_" + str(topic_num)
        consumer_ids[topic_name] = []
        for _ in range(random.randint(2, 5)):
            consumer_ids[topic_name].append(test_register_consumer(topic_name))
