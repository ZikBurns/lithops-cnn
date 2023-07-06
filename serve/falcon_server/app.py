import json
import falcon
from lithops.serve.serve import LithopsServe

class PredictResource:
    def on_post(self, req, resp):
        payload = json.loads(req.bounded_stream.read().decode('utf-8'))
        # Process the payload or access specific fields
        payload = json.loads(payload)
        print(payload)
        print(type(payload))
        # Simulate processing and generating a result
        server = LithopsServe(clean=False, config=payload["config"])
        result = server.run_orchestrator(payload)
        print(result)

        resp.body = json.dumps(result)
        resp.status = falcon.HTTP_200
        resp.content_type = 'application/json'

app = falcon.App()
predict_resource = PredictResource()
app.add_route('/off-sample/predict', predict_resource)

if __name__ == '__main__':
    from wsgiref import simple_server
    httpd = simple_server.make_server('localhost', 8000, app)
    httpd.serve_forever()