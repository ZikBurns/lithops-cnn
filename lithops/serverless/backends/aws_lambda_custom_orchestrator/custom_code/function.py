import json
import requests
import boto3
from lithops import FunctionExecutor
from time import time
from concurrent.futures import ThreadPoolExecutor
import copy
import os
import asyncio


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
    fexec = FunctionExecutor(reset=event["reset"], config=lithops_config, runtime_memory=3008)
    end = time()
    time_fexec = end - start
    print("FunctionExecutor time: ", time_fexec, " secs")
    print(payload_list)
    start = time()
    result = fexec.map_cnn_threading(payload_list)
    end = time()
    print(result)
    print("Map time: ", end - start, " secs")
    fexec.close()
    return {
        'statusCode': 200,
        'body': json.dumps(
            ["Map time: " + str(end - start) + " seconds", "FunctionExecutor time: " + str(time_fexec) + " secs",
             result])
    }


def lambda_function_benchmark(event):
    payload = event["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    payload_list = []
    chunk_size = payload['chunk_size']

    grouped = [payload['images'][i:i + chunk_size] for i in range(0, len(payload['images']), chunk_size)]

    for chunk in grouped:
        payload_list.append({'payload': {'body': {'images': chunk}}})

    lithops_config = event["config"].copy()
    lithops_config["lithops"]["backend"] = "aws_lambda_custom"
    fexec = FunctionExecutor(reset=event["reset"], config=lithops_config, runtime_memory=3008)

    print(payload_list)
    force_cold = event["force_cold"]
    result, times = fexec.map_sync(payload_list, force_cold=force_cold)

    print(result)
    fexec.close()
    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'times': json.dumps(times)
    }


def lambda_function_benchmark_batch(event):
    payload = event["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    payload_list = []
    chunk_size = payload['chunk_size']

    grouped = [payload['images'][i:i + chunk_size] for i in range(0, len(payload['images']), chunk_size)]

    for chunk in grouped:
        payload_list.append({'payload': {'body': {'images': chunk}}})
    lithops_config = event["config"].copy()
    lithops_config["lithops"]["backend"] = "aws_lambda_custom"
    fexec = FunctionExecutor(reset=event["reset"], config=lithops_config, runtime_memory=3008)
    start = time()
    force_cold = event["force_cold"]
    result, invocation_times = fexec.map_sync(payload_list, force_cold=force_cold)
    end = time()
    print(result)
    fexec.close()
    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'invocation_times': invocation_times
    }


def lambda_function_benchmark_batch_streaming(event):
    payload = event["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    payload_list = []
    chunk_size = payload['chunk_size']
    stream_size = payload['stream_size']
    grouped = [payload['images'][i:i + chunk_size] for i in range(0, len(payload['images']), chunk_size)]

    for chunk in grouped:
        payload_list.append({'payload': {'body': {'images': chunk, 'stream_size': stream_size}}})
    lithops_config = event["config"].copy()
    lithops_config["lithops"]["backend"] = "aws_lambda_custom"
    fexec = FunctionExecutor(reset=event["reset"], config=lithops_config, runtime_memory=3008)
    start = time()
    force_cold = event["force_cold"]
    result, invocation_times = fexec.map_sync(payload_list, force_cold=force_cold)
    end = time()
    print(result)
    fexec.close()
    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'invocation_times': invocation_times
    }


def lambda_function_benchmark_batch_split_streaming(event):
    payload = event["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    payload_list = []
    chunk_size = payload['chunk_size']
    download_stream_size = payload['download_stream_size']
    inference_stream_size = payload['inference_stream_size']
    grouped = [payload['images'][i:i + chunk_size] for i in range(0, len(payload['images']), chunk_size)]

    for chunk in grouped:
        payload_list.append({'payload': {'body': {'images': chunk, 'download_stream_size': download_stream_size,
                                                  "inference_stream_size": inference_stream_size}}})
    lithops_config = event["config"].copy()
    lithops_config["lithops"]["backend"] = "aws_lambda_custom"
    fexec = FunctionExecutor(reset=event["reset"], config=lithops_config, runtime_memory=3008)
    start = time()
    force_cold = event["force_cold"]
    result, invocation_times = fexec.map_sync(payload_list, force_cold=force_cold)
    end = time()
    print(result)
    fexec.close()
    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'invocation_times': invocation_times
    }


def upload_list_to_s3(my_list, upload_url):
    # Convert the list to JSON
    json_data = json.dumps(my_list)

    # Upload the JSON data to the pre-signed URL
    response = requests.put(upload_url, data=json_data)
    if response.status_code != 200:
        print(f"Error uploading to S3: {response.text}")
    else:
        print("Upload successful")


def lambda_function_individual(fexec, event):
    # print(event)
    payload = event["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    print("Images here:", len(payload["images"]))
    payload_list = []
    chunk_size = payload['chunk_size']

    grouped = [payload['images'][i:i + chunk_size] for i in range(0, len(payload['images']), chunk_size)]

    for chunk in grouped:
        payload_list.append({'payload': {'body': {'images': chunk}}})

    # print("Payload list", payload_list)
    result = fexec.map_cnn_threading(payload_list)
    print(result)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }


def lambda_function_individual_fexec(fexec, event):
    # print(event)
    payload = event["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    print("Images here:", len(payload["images"]))
    payload_list = []
    chunk_size = payload['chunk_size']

    grouped = [payload['images'][i:i + chunk_size] for i in range(0, len(payload['images']), chunk_size)]

    for chunk in grouped:
        payload_list.append({'payload': {'body': {'images': chunk}}})

    result = fexec.map_cnn_threading(payload_list)

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }


def lambda_function_upload(args):
    fexec = args[0]
    event = args[1]
    print(event)
    result = lambda_function_individual(fexec, event)
    upload_url = event["result_url"]
    print("Results will be in: ", upload_url)
    upload_list_to_s3(result, upload_url)
    return result


def joined_general_executor(fexec, events):
    first_event = events[0]
    joined_event = copy.deepcopy(first_event)
    joined_event["body"]["images"] = []
    for event in events:
        joined_event["body"]["images"].extend(event["body"]["images"])
    divided_results = []
    results = lambda_function_individual(fexec, joined_event)
    results_list = json.loads(results["body"])
    for event in events:
        num_images = len(event["body"]["images"])
        event_results = results_list[:num_images]
        print(event_results)
        del results_list[:num_images]
        upload_url = event["result_url"]
        upload_list_to_s3(event_results, upload_url)
        divided_results.append(event_results)
    return divided_results


def little_general_executor(fexec, events):
    events_fexec = [(fexec, event) for event in events]
    print(events_fexec)
    with ThreadPoolExecutor(max_workers=len(events_fexec)) as executor:
        results = list(executor.map(lambda_function_upload, events_fexec))
    return results


async def general_executor_individual(event):
    with ThreadPoolExecutor(max_workers=1) as executor:
        results = list(executor.map(lambda_function_individual, [event]))
    return results


def process_big_queue(fexec, events):
    results = []
    for event in events:
        result = lambda_function_individual(fexec, event)
        results.append(result)
    upload_url = events[0]["result_url"]
    print("Results will be in: ", upload_url)
    upload_list_to_s3(results, upload_url)
    return results


def lambda_function_sqs(records):
    lambda_client = boto3.client('lambda')
    print("messages sent: ", len(records))
    first_event = json.loads(records[0]["body"])
    lithops_config = first_event["config"].copy()
    lithops_config["lithops"]["backend"] = "aws_lambda_custom"
    fexec = FunctionExecutor(config=lithops_config, runtime_memory=3008)
    fexec.select_runtime()
    thread_events = []
    current_queue = 0
    results = []
    for record in records:
        event = json.loads(record["body"])
        response = lambda_client.get_account_settings()
        unreserved_concurrency = response['AccountLimit']['UnreservedConcurrentExecutions']
        # unreserved_concurrency=900
        print("Concurrency", unreserved_concurrency)
        len_images_record = len(event["body"]["images"])
        print("Images in record", len_images_record)
        if (len_images_record < unreserved_concurrency):
            if (current_queue + len_images_record > unreserved_concurrency):
                result = little_general_executor(fexec, thread_events)
                # print(result)
                results.append(result)
                current_queue = 0
                thread_events = []
            thread_events.append(event)
            current_queue = current_queue + len_images_record
        else:
            if (current_queue > 0):
                result = little_general_executor(fexec, thread_events)
                results.append(result)
                current_queue = 0
                thread_events = []
            grouped = [event["body"]["images"][i:i + unreserved_concurrency] for i in
                       range(0, len(event["body"]["images"]), unreserved_concurrency)]
            for group in grouped:
                tmp_event = copy.deepcopy(event)
                tmp_event["body"]["images"] = group
                thread_events.append(tmp_event)
            result = process_big_queue(fexec, thread_events)
            results.append(result)
            current_queue = 0
            thread_events = []
    if (current_queue > 0):
        print("Current queue: ", current_queue)
        result = little_general_executor(fexec, thread_events)
        current_queue = 0
        thread_events = []
        # result=lambda_function_individual(event)
        results.append(result)
    fexec.close()
    return results


def lambda_function_sqs_serial(records):
    lambda_client = boto3.client('lambda')
    print("messages sent: ", len(records))
    first_event = json.loads(records[0]["body"])
    lithops_config = first_event["config"].copy()
    lithops_config["lithops"]["backend"] = "aws_lambda_custom"
    fexec = FunctionExecutor(config=lithops_config, runtime_memory=3008)

    results = []
    for record in records:
        thread_events = []
        event = json.loads(record["body"])
        response = lambda_client.get_account_settings()
        unreserved_concurrency = response['AccountLimit']['UnreservedConcurrentExecutions']
        # unreserved_concurrency=900
        print("Concurrency", unreserved_concurrency)
        len_images_record = len(event["body"]["images"])
        print("Images in record", len_images_record)
        if (len_images_record < unreserved_concurrency):
            thread_events.append(event)
            result = little_general_executor(fexec, thread_events)
            # print(result)
            results.append(result)
        else:
            grouped = [event["body"]["images"][i:i + unreserved_concurrency] for i in
                       range(0, len(event["body"]["images"]), unreserved_concurrency)]
            for group in grouped:
                tmp_event = copy.deepcopy(event)
                tmp_event["body"]["images"] = group
                thread_events.append(tmp_event)
            result = process_big_queue(fexec, thread_events)
            results.append(result)
            thread_events = []
    fexec.close()
    return results

