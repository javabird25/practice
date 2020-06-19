import json
from flask import Flask, request

app = Flask(__name__)


@app.route("/client/info")
def user_agent():
    return json.dumps({"user_agent": request.headers["user-agent"]})
