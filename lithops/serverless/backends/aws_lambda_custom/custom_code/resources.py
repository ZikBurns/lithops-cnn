import requests
from lithops.serverless.backends.aws_lambda_custom.custom_code.model import OffSamplePredictModel
import boto3
import time
import botocore
from lithops.serverless.backends.aws_lambda_custom.custom_code.transformations import  transform_images, transform_image, CustomAffineGridSample


class PredictResource:
    def __init__(self, model_path, batch_size=32):
        start = time.time()
        self.model = OffSamplePredictModel(model_path)
        end = time.time()
        self.time_model_init = end - start
        print("time_model_init", self.time_model_init)
        self.images_limit = batch_size
        client_config = botocore.config.Config(
            max_pool_connections=1000,
        )
        self.s3_client = boto3.client('s3', config=client_config)

    def get_time_model_init(self):
        return self.time_model_init

    def downloadimages(self, urls, s3_bucket):
        images_data = []
        if 'http' in urls[0]:
            for url in urls:
                response = requests.get(url)
                images_data.append(response.content)
        else:
            for key in urls:
                response = self.s3_client.get_object(Bucket=s3_bucket, Key=key)
                object_data = response['Body'].read()
                images_data.append(object_data)
        return images_data
        
    def downloadimage(self, key,s3_bucket):
        response = self.s3_client.get_object(Bucket=s3_bucket, Key=key)
        object_data = response['Body'].read()
        return object_data
            
    def transform_images(self, images):
        return transform_images(images)


    def transform_image(self, image):
        return transform_image(image)


    def predict(self, images):
        probs, labels = self.model.predict(images)
        pred_list = [{'prob': float(prob), 'label': label} for prob, label in zip(probs, labels)]
        return {'predictions': pred_list}

    def execute_inference(self, payload, s3_bucket):
        images_data = self.downloadimages(payload["images"], s3_bucket)
        resp_doc = self.predict(images_data)
        return resp_doc

    def execute_inference_benchmark(self, payload, s3_bucket):
        start = time.time()
        images_data = self.downloadimages(payload["images"], s3_bucket)
        end = time.time()
        time_download = end - start
        start = time.time()
        resp_doc = self.predict(images_data)
        end = time.time()
        time_inference = end - start
        return resp_doc, time_download, time_inference, self.time_model_init