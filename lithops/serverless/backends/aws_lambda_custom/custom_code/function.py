import json
from lithops.serverless.backends.aws_lambda_custom.custom_code.resources import PredictResource
import time
import concurrent.futures
import threading
import sys
from queue import Queue
import concurrent


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
#     num_download_threads= int(stream_size_inference/stream_size_download)
#     arguments = [arguments[i:i + num_download_threads] for i in range(0, len(arguments), num_download_threads)]
#     downloaded_images = []

#     def execute_chunk(images_data):
#         predictions = predict_resource.predict(images_data)
#         result = {'predictions': predictions['predictions']}
#         return result

#     def download_images(argument, count, downloaded_images):
#         print(f"Thread {count} is starting download.\n")
#         s3_bucket = "off-sample"
#         images_data = predict_resource.downloadimages(argument["images"], s3_bucket)
#         print(f"Thread {count} finished download.\n")
#         for image_data in images_data:
#             downloaded_images.append(image_data)

#     start=time.time()
#     results=[]

#     for count_inference, argument in enumerate(arguments, 0):
#         threads = []
#         for count_download, image_dict in enumerate(argument):
#             thread = threading.Thread(target=download_images, args=(image_dict, str(count_inference), downloaded_images))
#             threads.append(thread)
#             thread.start()
#         if len(downloaded_images)>=stream_size_inference:
#             images_data = downloaded_images[:stream_size_inference]
#             del downloaded_images[:stream_size_inference]
#             print(f"Inference {count_inference} started\n")
#             result = execute_chunk(images_data)
#             results.append(result)
#             print(f"Inference {count_inference} ended\n")
#         for thread in threads:
#             thread.join()

#     count_inference=count_inference+1
#     images_data = downloaded_images[:stream_size_inference]
#     del downloaded_images[:stream_size_inference]
#     print(f"Inference {count_inference} started\n")
#     result = execute_chunk(images_data)
#     results.append(result)
#     print(f"Inference {count_inference} ended\n")
#     end=time.time()

#     predictions = []
#     for result in results:
#         predictions.append(result)

#     total_time = end - start
#     print("Total time:", total_time)
#     print(sys.getsizeof(predictions))
#     print(predictions)
#     return {
#         'statusCode': 200,
#         'body': predictions,
#         'total_time': total_time
#     }


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


def divide_list_into_chunks(input_list, chunk_size):
    """Divide a list into N equal-sized chunks."""
    if chunk_size <= 0:
        raise ValueError("Chunk size should be a positive integer.")
    if chunk_size >= len(input_list):
        return [input_list]

    # Calculate the number of chunks needed
    num_chunks = len(input_list) // chunk_size + (len(input_list) % chunk_size > 0)

    # Use list comprehension to create the chunks
    chunks = [input_list[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

    return chunks


# Split-Pipelining-threadingpool
# def lambda_function(payload, s3_bucket):
#     predict_resource = PredictResource("/opt/model.pt")
#     payload = payload["body"]
#     if isinstance(payload, str):
#         payload = json.loads(payload)
#     urls_general = payload["images"]
#     print(payload)
#     stream_size_download = payload["download_stream_size"]
#     stream_size_inference = payload["inference_stream_size"]
#     queue = Queue(maxsize=stream_size_inference)


#     def execute_chunk(images_data):
#         predictions = predict_resource.predict(images_data)
#         result = {'predictions': predictions['predictions']}
#         return result

#     def download_images(args):
#         predict_resource, arguments, stream_size_download, queue = args
#         arguments = divide_list_into_chunks(arguments, stream_size_download)
#         for images in arguments:
#             print(f"Download started: {images}\n")
#             s3_bucket = "off-sample"
#             images_data = predict_resource.downloadimages(images,s3_bucket)
#             print(f"Download  finished.\n")
#             for image_data in images_data:
#                 queue.put(image_data)
#     start=time.time()
#     results=[]
#     download_freq = int(-(-(stream_size_inference / stream_size_download) // 1))
#     division = int(len(urls_general) / download_freq)
#     download_arguments = divide_list_into_chunks(urls_general, division)

#     with concurrent.futures.ThreadPoolExecutor(max_workers=len(download_arguments)) as executor:
#         producer_threads = [executor.submit(download_images, (predict_resource,argument, stream_size_download, queue)) for argument in download_arguments]

#         num_inferences =  int(-(-(len(urls_general)  / stream_size_inference) // 1))
#         for count_inference in range(num_inferences):
#             stream=[]
#             for i in range(stream_size_inference):
#                 value = queue.get()
#                 if not value:
#                     queue.put(value)
#                 stream.append(value)
#             if stream:
#                 print(f"Inference started\n")
#                 result = execute_chunk(stream)
#                 results.append(result)
#                 print(f"Inference ended\n")
#         for producer_thread in concurrent.futures.as_completed(producer_threads):
#             pass

#     end=time.time()

#     predictions = []
#     for result in results:
#         predictions.append(result)

#     total_time = end - start
#     print("Total time:", total_time)
#     print(sys.getsizeof(predictions))
#     print(predictions)
#     return {
#         'statusCode': 200,
#         'body': predictions,
#         'total_time': total_time
#     }


# Parallel Split Pipelining Threadpooling
def lambda_function(payload, s3_bucket):
    predict_resource = PredictResource("/opt/model.pt")
    payload = payload["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    urls_general = payload["images"]
    arguments = []
    stream_size_download = payload["download_stream_size"]
    stream_size_inference = payload["inference_stream_size"]

    for i in range(0, len(urls_general), stream_size_download):
        chunk = urls_general[i:i + stream_size_download]
        argument = {"images": chunk}
        arguments.append(argument)

    def producer_task(args):
        queue, predict_resource, argument = args
        image_datas = load_images(argument, predict_resource)
        # push data into queue
        for image_data in image_datas:
            queue.put(image_data)

    # producer manager task
    def producer_manager(queue, predict_resource, arguments):
        # create thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(arguments)) as executor:
            # use threads to generate items and put into the queue
            producer_threads = [executor.submit(producer_task, (queue, predict_resource, argument)) for
                                argument in arguments]
            for producer_thread in concurrent.futures.as_completed(producer_threads):
                pass
        # put a signal to expect no further tasks
        queue.put(None)
        # report a message
        print('>producer_manager done.')

    # consumer task
    def consumer_task(args):
        queue, stream_size_inference = args
        while True:
            stream = []
            for _ in range(stream_size_inference):
                value = queue.get()
                if not value:
                    queue.put(value)
                    return
                stream.append(value)
            result = execute_chunk(stream)
            return result

    # consumer manager
    def consumer_manager(queue, num_images, stream_size_inference, results):
        distributions = distribute_number(num_images, stream_size_inference)
        # create thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(distributions)) as executor:
            consumer_threads = [executor.submit(consumer_task, (queue, distribution)) for distribution in distributions]
            # wait for all tasks to complete
            for consumer_thread in concurrent.futures.as_completed(consumer_threads):
                pass
        for consumer_thread in consumer_threads:
            results.append(consumer_thread.result())

    def execute_chunk(images_data):
        print(f"Inference started.\n")
        predictions = predict_resource.predict(images_data)
        result = {'predictions': predictions['predictions']}
        print(f"Inference finished.\n")
        return result

    def download_images(argument, predict_resource):
        print(f"Download started.\n")
        s3_bucket = "off-sample-eu"
        images_data = predict_resource.downloadimages(argument["images"], s3_bucket)
        print(f"Download finished.\n")
        return images_data

    def load_images(argument, predict_resource):
        print(f"Download started.\n")
        s3_bucket = "off-sample"
        images_data = predict_resource.downloadimages(argument["images"], s3_bucket)
        print(f"Download finished.\n")
        return images_data

    def distribute_number(number, batch_size):
        result = []

        while number > 0:
            if number >= batch_size:
                result.append(batch_size)
                number -= batch_size
            else:
                result.append(number)
                number = 0

        return result

    def download_images(args):
        predict_resource, arguments, stream_size_download, queue = args
        arguments = divide_list_into_chunks(arguments, stream_size_download)
        for images in arguments:
            print(f"Download started: {images}\n")
            s3_bucket = "off-sample"
            images_data = predict_resource.downloadimages(images, s3_bucket)
            print(f"Download  finished.\n")
            for image_data in images_data:
                queue.put(image_data)

    results = []
    start = time.time()
    # create the shared queue
    queue = Queue()
    # run the consumer
    consumer = threading.Thread(target=consumer_manager,
                                args=(queue, len(urls_general), stream_size_inference, results,))
    consumer.start()
    # run the producer
    producer = threading.Thread(target=producer_manager, args=(queue, predict_resource, arguments,))
    producer.start()
    # wait for the producer to finish
    producer.join()
    # wait for the consumer to finish
    consumer.join()

    end = time.time()

    predictions = []
    for result in results:
        predictions.append(result)

    total_time = end - start
    print("Total time:", total_time)
    print(sys.getsizeof(predictions))
    print(predictions)
    return {
        'statusCode': 200,
        'body': predictions,
        'total_time': total_time
    }