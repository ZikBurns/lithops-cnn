import json
import boto3
from lithops import FunctionExecutor
from time import time


def lambda_function(event):
    payload = event["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    payload_list = []
    chunk_size = payload['chunk_size']

    grouped = [payload['images'][i:i + chunk_size] for i in range(0, len(payload['images']), chunk_size)]

    for chunk in grouped:
        payload_list.append({'payload': {'body': {'images': chunk}}})
    start = time()
    lithops_config = event["config"].copy()
    lithops_config["lithops"]["backend"] = "aws_lambda_custom"
    fexec = FunctionExecutor(reset = event["reset"],config = lithops_config, runtime_memory=3008)
    end = time()
    time_fexec = end - start
    print("FunctionExecutor time: ", time_fexec, " secs")

    start = time()
    result = fexec.map_cnn_threading(payload_list)
    end = time()
    print(result)
    print("Map time: ", end - start, " secs")
    return {
        'statusCode': 200,
        'body': json.dumps(["Map time: " + str(end - start) + " seconds", "FunctionExecutor time: " + str(time_fexec) + " secs", result])
    }

def lambda_function_sqs(records):
    lambda_client = boto3.client('lambda')
    response = lambda_client.get_account_settings()
    unreserved_concurrency = response['AccountLimit']['UnreservedConcurrentExecutions']
    print(unreserved_concurrency)
    print(len(records))
    results=[]
    for record in records:
        event=json.loads(record["body"])
        result=lambda_function(event)
        results.append(result)
    return results
