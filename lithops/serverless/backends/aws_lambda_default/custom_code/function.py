import json
from lithops.serverless.backends.aws_lambda_custom_image.custom_code.resources import PredictResource
import time
import concurrent.futures
import threading
import sys
from queue import Queue
import concurrent
import torch
import os
from lithops.serverless.backends.aws_lambda_custom_image.custom_code.scheduler.task_scheduler import TaskScheduler
from lithops.serverless.backends.aws_lambda_custom_image.custom_code.model import OffSampleTorchscriptFork
import logging
import grpc
from lithops.serverless.backends.aws_lambda_custom_image.custom_code.grpc_assets import urlrpc_pb2
from lithops.serverless.backends.aws_lambda_custom_image.custom_code.grpc_assets import urlrpc_pb2_grpc
import socket
import boto3 


start = time.time()
jit_model = torch.jit.load("/function/bin/model.pt", torch.device('cpu'))
print(f"Model loading took {time.time() - start} seconds")
resources = PredictResource()

S3_BUCKET="off-sample-eu"
config_dict = {
               'load': {'batch_size': 0, 'max_concurrency': 0},
               'preprocess': {'batch_size': 0, 'num_cpus': 0},
               'predict': {'interop':0, 'intraop': 0, 'n_models': 0}
}
inferencer = TaskScheduler(config_dict=config_dict, logging_level=logging.INFO)

@inferencer.task(mode="threading")
def load(image_dict):
    result_dict = {}
    for key in image_dict:
        print(f"Downloading image {key} from {S3_BUCKET}")
        image_data = resources.downloadimage(key, S3_BUCKET)
        print("Downloading image", key)
        result_dict.update({key: image_data})
    return result_dict

@inferencer.task(mode="multiprocessing", previous=load, batch_format="bytes")
def preprocess(image_dict):
    result_dict = {}
    for key, value in image_dict.items():
        print("Transformation started", key)
        tensor = resources.transform_image(value)
        result_dict.update({key: tensor})
        print("Transformation finished", key)
    return result_dict

@inferencer.task(mode="torchscript", previous=preprocess, batch_format="tensor", jit_model=jit_model)
def predict(tensor_dicts, ensemble):
    print("Predicting images")
    tensors = []
    for key, value in tensor_dicts.items():
        tensors.append(value)
    prediction_results = OffSampleTorchscriptFork(ensemble).predict(tensors)
    result_dict = {}
    for key, prediction_result in zip(tensor_dicts.keys(), prediction_results):
        result_dict.update({key: prediction_result})
    return result_dict



def get_private_ip_by_name(instance_name):
    # Create an EC2 client
    ec2_client = boto3.client('ec2')

    # Get the private IP address based on the instance name tag
    response = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}])

    # Check if any instances were found
    if 'Reservations' in response and response['Reservations']:
        instance = response['Reservations'][0]['Instances'][0]
        private_ip = instance['PrivateIpAddress']
        return private_ip
    else:
        return None
    
    
def lambda_function(payload):
    print(payload)
    # time.sleep(5)
    return {
        'statusCode': 200,
        'body': "Function just returns after loading dependencies"
    }
    S3_BUCKET='off-sample-eu'
    if "WARM_START_FLAG" in os.environ:
        is_cold_start = False
    else:
        is_cold_start = True
        os.environ["WARM_START_FLAG"] = "True"

    payload = payload["body"]
    s3_bucket = payload["bucket"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    print(payload)

    if 'config' in payload.keys():
        config_dict = payload["config"]
    
    time_log=None
    if 'grpc_port' in payload:
        port = payload['grpc_port']
        print("gRPC connecting to", port)
        rpc_dict = urlrpc_pb2.Dict(key="", value="")
        channel = grpc.insecure_channel(f'10.0.6.79:{port}')
        
        
        stub = urlrpc_pb2_grpc.URLRPCStub(channel)
        
        all_results = []
        batch = 1
        rpc_dict = urlrpc_pb2.Dict(key="", value="")
        finished_urls = [rpc_dict]
        while batch:
            response = stub.Add(urlrpc_pb2.urlRequest(finished=finished_urls))
            finished_urls = []
            print(response.urls)
            batch = response.urls
            if batch:
                url_dicts = {}
                for url in batch:
                    url_dicts.update({url: None})

                prediction_dicts = inferencer.process_tasks(url_dicts, config_dict)

                for key, result in prediction_dicts.items():
                    rpc_dict = urlrpc_pb2.Dict(key=key, value=str(result))
                    all_results.append({key: str(result)})
                    finished_urls.append(rpc_dict)
        result = {'predictions': all_results}
    else:
        print("Scheduler defined")
        batch = payload['images']
        limit = payload['limit']
        path_lists = [batch[i:i + limit] for i in range(0, len(batch), limit)]
        result_dicts = {}
        time_logs = []
        for path_list in path_lists:
            url_dicts = {}
            for url in path_list:
                url_dicts.update({url: None})

            print("Processing ")
            start = time.time()
            prediction_dicts = inferencer.process_tasks(url_dicts, config_dict,'/tmp/time_log.txt')
            print("Finished tasks took ", time.time() - start)
            time_log = inferencer.get_log_file_content()
            time_logs.append(time_log)
            print("Finished tasks")

            for key, result in prediction_dicts.items():
                result_dicts.update({key: str(result)})

        result = {'predictions': result_dicts}
        print(result)

    return {
        'statusCode': 200,
        'body': result,
        'time_log': time_log
    }
