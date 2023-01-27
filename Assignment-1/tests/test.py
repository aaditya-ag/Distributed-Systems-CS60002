import requests

base_url = 'http://127.0.0.1:5000/'

def test_get_topics():
    print(50*"-")
    print(":: Testing Get Topics ::")
    print(50*"-")
    resp = requests.get(base_url+"topics")
    # assert(resp.status_code == 200)
    data = resp.json()
    # assert(data == ['hello', 'bye'])
    print(data)
    print(50*"-")

def test_create_topic():
    print(50*"-")
    print(":: Testing Create Topic ::")
    print(50*"-")
    data = {
        "name": "test_topic_1"
    }
    resp = requests.post(url=base_url+"topics",json=data)
    print(resp.status_code)
    print(50*"-")

if __name__ == '__main__':
    test_create_topic()
    test_get_topics()