import json
from lithops.serverless.backends.aws_lambda_custom.custom_code.resources import PredictResource
import time
import concurrent.futures
import threading
import sys


# def lambda_function(payload, s3_bucket):
#     predict_resource = PredictResource("/opt/model.pt")
#     # time_model_init=predict_resource.get_time_model_init()
#     # return {
#     #     'statusCode': 200,
#     #     'time_model_init': time_model_init
#     # }
#     payload = payload["body"]
#     if isinstance(payload, str):
#         payload = json.loads(payload)
#     # predictions = PredictResource("/opt/model.pt").execute_inference(payload, s3_bucket)
#     # result = {'predictions': predictions['predictions']}

#     start=time.time()
#     predictions, time_download, time_inference, time_model_init= predict_resource.execute_inference_benchmark(payload, s3_bucket)
#     end=time.time()
#     print("Total time:",end-start)
#     result = {'predictions': predictions['predictions'], 'time_download': time_download,  'time_inference':time_inference, 'time_model_init': time_model_init}
#     return {
#         'statusCode': 200,
#         'body': result
#     }


# Compact threading
# def lambda_function(payload, s3_bucket):
#     predict_resource = PredictResource("/opt/model.pt")
#     payload = payload["body"]
#     if isinstance(payload, str):
#         payload = json.loads(payload)
#     urls_general=payload["images"]
#     stream_size=payload["stream_size"]
#     arguments = []
#     for i in range(0, len(urls_general), stream_size):
#         chunk = urls_general[i:i + stream_size]
#         argument = {"images": chunk}
#         arguments.append(argument)

#     def execute_chunk(argument):
#         s3_bucket="off-sample"
#         predictions, time_download, time_inference, time_model_init= predict_resource.execute_inference_benchmark(argument, s3_bucket)
#         result = {'predictions': predictions['predictions'], 'time_download': time_download,  'time_inference':time_inference, 'time_model_init': time_model_init}
#         return result

#     start=time.time()
#     with concurrent.futures.ThreadPoolExecutor(max_workers=len(arguments)) as executor:
#         results_theads = executor.map(execute_chunk, arguments)
#     end=time.time()
#     results=[]
#     for argument, result in zip(arguments, results_theads):
#         results.append(result)
#     total_time=end-start
#     print("Total time:",total_time)
#     return {
#         'statusCode': 200,
#         'body': results,
#         'total_time': total_time
#     }


# Split threading
# def lambda_function(payload, s3_bucket):
#     predict_resource = PredictResource("/opt/model.pt")
#     payload = payload["body"]
#     if isinstance(payload, str):
#         payload = json.loads(payload)
#     urls_general=payload["images"]
#     stream_size=payload["stream_size"]
#     arguments = []
#     for i in range(0, len(urls_general), 2):
#         chunk = urls_general[i:i + 2]
#         argument = {"images": chunk}
#         arguments.append(argument)

#     downloaded_images=[]
#     predictions = []

#     def download_images(argument):
#         s3_bucket = "off-sample"
#         images_data = predict_resource.downloadimages(argument["images"], s3_bucket)
#         downloaded_images.extend(images_data)

#     def predict_images(argument):
#         prediction = predict_resource.predict(argument)
#         return prediction


#     start=time.time()
#     with concurrent.futures.ThreadPoolExecutor(max_workers=len(arguments)) as executor:
#         executor.map(download_images, arguments)
#     end=time.time()
#     time_download = end-start


#     print(len(downloaded_images))
#     images_to_inference=[]
#     for i in range(0, len(downloaded_images), stream_size):
#         chunk = downloaded_images[i:i + stream_size]
#         images_to_inference.append(chunk)

#     start=time.time()
#     with concurrent.futures.ThreadPoolExecutor(max_workers=len(images_to_inference)) as executor:
#         predictions = executor.map(predict_images, images_to_inference)
#     end=time.time()
#     time_inference = end-start


#     results=[]
#     for result in predictions:
#         results.append(result)
#     total_time=time_inference+time_download
#     print("Total time:",total_time)
#     return {
#         'statusCode': 200,
#         'body': results,
#         'total_time': total_time,
#         'download_time': time_download,
#         'inference_time': time_inference
#     }


# Pipelining
# def lambda_function(payload, s3_bucket):
#     predict_resource = PredictResource("/opt/model.pt")
#     payload = payload["body"]
#     if isinstance(payload, str):
#         payload = json.loads(payload)
#     urls_general=payload["images"]
#     stream_size=payload["stream_size"]
#     arguments = []
#     for i in range(0, len(urls_general), stream_size):
#         chunk = urls_general[i:i + stream_size]
#         argument = {"images": chunk}
#         arguments.append(argument)

#     downloaded_images=[]

#     def execute_chunk(images_data):
#         predictions = predict_resource.predict(images_data)
#         result = {'predictions': predictions['predictions']}

#         return result

#     def download_images(argument, count, downloaded_images):
#         print(f"Thread {count} is starting download.\n")
#         s3_bucket = "off-sample"
#         images_data = predict_resource.downloadimages(argument["images"], s3_bucket)
#         print(f"Thread {count} finished download.\n")
#         return_value = count , images_data
#         downloaded_images.append(return_value)


#     start=time.time()
#     predictions=[]

#     for count, argument in enumerate(arguments, 1):
#         thread = threading.Thread(target=download_images, args=(argument, count, downloaded_images))
#         thread.start()
#         if len(downloaded_images)>0:
#             downloaded_count, images_data = downloaded_images.pop(0)
#             print(f"Inference {downloaded_count} started\n")
#             result = execute_chunk(images_data)
#             predictions.append(result)
#             print(f"Inference {downloaded_count} ended\n")
#         thread.join()


#     downloaded_count, images_data = downloaded_images.pop(0)
#     print(f"Inference {downloaded_count} started\n")
#     result = execute_chunk(images_data)
#     predictions.append(result)
#     print(f"Inference {downloaded_count} ended\n")
#     end=time.time()
#     results=[]
#     for result in predictions:
#         results.append(result)

#     total_time=end-start
#     print("Total time:",total_time)
#     print(sys.getsizeof(results))
#     print(results)
#     return {
#         'statusCode': 200,
#         'body': results,
#         'total_time': total_time
#     }


# Split-Pipelining
def lambda_function(payload, s3_bucket):
    predict_resource = PredictResource("/opt/model.pt")
    payload = payload["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    urls_general = payload["images"]
    print(payload)
    stream_size_download = payload["download_stream_size"]
    stream_size_inference = payload["inference_stream_size"]
    arguments = []
    for i in range(0, len(urls_general), stream_size_download):
        chunk = urls_general[i:i + stream_size_download]
        argument = {"images": chunk}
        arguments.append(argument)
    arguments = [arguments[i:i + stream_size_inference] for i in range(0, len(arguments), stream_size_inference)]
    downloaded_images = []

    def execute_chunk(images_data):
        predictions = predict_resource.predict(images_data)
        result = {'predictions': predictions['predictions']}
        return result

    def download_images(argument, count, downloaded_images):
        print(f"Thread {count} is starting download.\n")
        s3_bucket = "off-sample"
        images_data = predict_resource.downloadimages(argument["images"], s3_bucket)
        print(f"Thread {count} finished download.\n")
        return_value = images_data[0]
        downloaded_images.append(return_value)

    start = time.time()
    predictions = []

    for count_inference, argument in enumerate(arguments, 1):
        threads = []
        for count_download, image_dict in enumerate(argument):
            thread = threading.Thread(target=download_images, args=(
            image_dict, str(count_inference) + "-" + str(count_download), downloaded_images))
            threads.append(thread)
            thread.start()
        if len(downloaded_images) >= stream_size_inference:
            images_data = downloaded_images[:stream_size_inference]
            del downloaded_images[:stream_size_inference]
            print(f"Inference {count_inference} started\n")
            result = execute_chunk(images_data)
            predictions.append(result)
            print(f"Inference {count_inference} ended\n")
        for thread in threads:
            thread.join()

    images_data = downloaded_images[:stream_size_inference]
    del downloaded_images[:stream_size_inference]
    print(f"Inference {count_inference} started\n")
    result = execute_chunk(images_data)
    predictions.append(result)
    print(f"Inference {count_inference} ended\n")
    end = time.time()

    results = []
    for result in predictions:
        results.append(result)

    total_time = end - start
    print("Total time:", total_time)
    print(sys.getsizeof(results))
    print(results)
    return {
        'statusCode': 200,
        'body': results,
        'total_time': total_time
    }


# Experimental Split-Pipelining
# def lambda_function(payload, s3_bucket):
#     predict_resource = PredictResource("/opt/model.pt")
#     payload = payload["body"]
#     if isinstance(payload, str):
#         payload = json.loads(payload)
#     urls_general = payload["images"]
#     print(payload)
#     stream_size_download = payload["download_stream_size"]
#     stream_size_inference = payload["inference_stream_size"]
#     arguments = []
#     for i in range(0, len(urls_general), stream_size_download):
#         chunk = urls_general[i:i + stream_size_download]
#         argument = {"images": chunk}
#         arguments.append(argument)
#     downloaded_images = []

#     def execute_chunk(images_data, results):
#         predictions = predict_resource.predict(images_data)
#         result = {'predictions': predictions['predictions']}
#         results.append(result)

#     def download_images(argument, count, downloaded_images):
#         print(f"Thread {count} is starting download.\n")
#         s3_bucket = "off-sample"
#         images_data = predict_resource.downloadimages(argument["images"], s3_bucket)
#         print(f"Thread {count} finished download.\n")
#         return_value = images_data[0]
#         downloaded_images.append(return_value)

#     start = time.time()
#     results = []

#     threads = []
#     for count_download, image_dict in enumerate(arguments):
#         thread = threading.Thread(target=download_images,
#                                   args=(image_dict, str(count_download) + "-" + str(count_download), downloaded_images))
#         threads.append(thread)
#         thread.start()

#     count_inference = 0
#     max_inferences = int(-(-(len(urls_general) / stream_size_inference) // 1))
#     while len(results) != max_inferences - 1:
#         if len(downloaded_images) >= stream_size_inference:
#             images_data = downloaded_images[:stream_size_inference]
#             del downloaded_images[:stream_size_inference]
#             print(f"Inference {count_inference} started\n")
#             thread = threading.Thread(target=execute_chunk,
#                                       args=(images_data, results))
#             threads.append(thread)
#             thread.start()
#             print(f"Inference {count_inference} ended\n")
#             count_inference = count_inference + 1

#     for thread in threads:
#         thread.join()

#     images_data = downloaded_images[:stream_size_inference]
#     del downloaded_images[:stream_size_inference]
#     print(f"Inference {count_inference} started\n")
#     execute_chunk(images_data, results)
#     print(f"Inference {count_inference} ended\n")

#     end = time.time()

#     results = []
#     for result in predictions:
#         results.append(result)

#     total_time = end - start
#     print("Total time:", total_time)
#     print(sys.getsizeof(results))
#     print(results)
#     return {
#         'statusCode': 200,
#         'body': results,
#         'total_time': total_time
#     }
