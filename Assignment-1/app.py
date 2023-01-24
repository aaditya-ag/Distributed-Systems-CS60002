from flask import Flask, jsonify, request

app = Flask(__name__)

# This is a basic app.py, doesn't have logic implemented in it

topics = [
    "hello",
    "bye"
]

@app.route('/topics', methods=['POST'])
def post_topics():
    topics.append(request.get_json()["name"])
    return "Done"

@app.route('/topics', methods=['GET'])
def get_topics():
    return jsonify(topics)

@app.route('/consumer/register', methods=['POST'])
def consumer_register():
    if request.get_json()["topic"] not in topics:
        return jsonify({
            "status": "Failure",
            "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        })
    return jsonify(topics)

@app.route('/consumer/consume', methods=['GET'])
def producer_produce():
    if request.get_json()["topic"] not in topics:
        return jsonify({
            "status": "Failure",
            "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        })
    return jsonify(topics)

@app.route('/producer/register', methods=['POST'])
def producer_register():
    if request.get_json()["topic"] not in topics:
        return jsonify({
            "status": "Failure",
            "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        })
    return jsonify(topics)

@app.route('/producer/produce', methods=['POST'])
def producer_produce():
    if request.get_json()["topic"] not in topics:
        return jsonify({
            "status": "Failure",
            "message": "Topic '" + request.get_json()["topic"] + "' doesn't exist."
        })
    return jsonify(topics)


if __name__ == '__main__':
    app.run(port=5000)