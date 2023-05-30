import requests
from model import OffSamplePredictModel

class PredictResource:
    def __init__(self, model_path, batch_size=32):
        self.model=OffSamplePredictModel(model_path)
        self.images_limit = batch_size

    def downloadimages(self, urls):
        images_data=[]
        for url in urls:
            response = requests.get(url)
            images_data.append(response.content)
        return images_data
    def predict(self, images):
        probs, labels = self.model.predict(images)
        pred_list = [{'prob': float(prob), 'label': label} for prob, label in zip(probs, labels)]
        return {'predictions': pred_list}

    def execute_inference(self, payload):
        images_data=self.downloadimages(payload["images"])
        resp_doc = self.predict(images_data)
        return resp_doc
