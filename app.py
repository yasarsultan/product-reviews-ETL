from flask import Flask, jsonify
import json

app = Flask(__name__)


@app.route("/api/reviews", methods=["GET"])
def get_reviews():
    with open("sourceB/reviews.json", "r") as file:
        data = json.load(file)
    
    return jsonify(data)

def api():
    app.run()

api()