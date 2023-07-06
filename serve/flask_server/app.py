import json

from flask import Flask, request
from lithops.serve.serve import LithopsServe
app = Flask(__name__)

@app.route('/off-sample/predict', methods=['POST'])
def predict():
    payload = json.loads(request.json)  # Retrieve the payload as JSON data

    # Process the payload or access specific fields
    print(type(payload))

    server = LithopsServe(clean=False,config = payload["config"])
    result = server.run_orchestrator(payload)
    print(result)
    response = app.response_class(
        response=json.dumps(result),
        status=200,
        mimetype='application/json'
    )
    return response

if __name__ == '__main__':
    app.run()