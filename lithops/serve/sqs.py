import json
import time
import uuid
from datetime import datetime
import boto3
import botocore
import requests
from botocore.exceptions import WaiterError, ClientError


class SQSManager:
    def __init__(self, config):
        self.api_name = config["api_gateway"]["api_name"]
        self.region_name = config["api_gateway"]["region_name"]
        self.aws_session = boto3.Session(
            aws_access_key_id=config["aws"]['access_key_id'],
            aws_secret_access_key=config["aws"]['secret_access_key'],
            aws_session_token=config["aws"].get('session_token'),
            region_name=self.region_name
        )
        self.sqs_client = self.aws_session.client('sqs', region_name=self.region_name)
        self.lambda_client = self.aws_session.client('lambda', region_name=self.region_name)
        self.s3_client = self.aws_session.client('s3', region_name=self.region_name)

    def create_queue(self, queue_name,visibility_timeout=43000):
        queue_url = self.sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={
                'FifoQueue': 'true',
                'ContentBasedDeduplication': 'true',
                'VisibilityTimeout': str(visibility_timeout)
            }
        )['QueueUrl']
        return queue_url

    def wait_for_queue_creation(self, queue_name):
        while True:
            try:
                response = self.sqs_client.get_queue_url(
                    QueueName=queue_name
                )
                self.queue_url = response['QueueUrl']
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                    print('Queue not yet created. Waiting...')
                    time.sleep(1)
                else:
                    raise
    def configure_trigger(self, queue_url, function_name, batch_size=1, max_batching_window=0):
        queue_arn = self.get_sqs_queue_arn(queue_url=queue_url)
        self.lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=batch_size,
            MaximumBatchingWindowInSeconds=max_batching_window,
            Enabled=True
        )


    def delete_trigger(self, queue_name, function_name):
        queue_url = self.get_queue_url(queue_name)
        queue_arn = self.get_sqs_queue_arn(queue_url)
        response = self.lambda_client.list_event_source_mappings(FunctionName=function_name)
        for mapping in response['EventSourceMappings']:
            if mapping['EventSourceArn'] == queue_arn:
                mapping_uuid = mapping['UUID']
                self.lambda_client.delete_event_source_mapping(UUID=mapping_uuid)
                break


    def delete_queue(self, queue_name):
        response = self.sqs_client.list_queues(QueueNamePrefix=queue_name)
        queue_urls = response.get('QueueUrls', [])
        for queue_url in queue_urls:
            if queue_url.endswith(queue_name):
                self.sqs_client.delete_queue(QueueUrl=queue_url)
                print(f"Deleted queue: {queue_url}")

    def queue_exists(self, queue_name):
        response = self.sqs_client.list_queues(QueueNamePrefix=queue_name)
        queue_urls = response.get('QueueUrls', [])
        return len(queue_urls) > 0 and queue_urls[0].endswith(queue_name)

    def get_s3_object_url(self,bucket_name, object_key):
        try:
            response = self.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=99999999999999999999999999  # URL expiration time in seconds
            )
            return response
        except Exception as e:
            print(f"Error generating pre-signed URL: {e}")
            return None


    def generate_presigned_url(self,bucket_name, object_key, operation):
        url = self.s3_client.generate_presigned_url(
            ClientMethod=operation,
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=3600
        )
        return url


    def send_message(self, queue_url, payload):
        message_deduplication_id = str(uuid.uuid4())
        object_key = "off-sample-results/"+message_deduplication_id
        pre_assigned_url = self.generate_presigned_url(payload["config"]["aws_s3"]["storage_bucket"],object_key,"put_object")
        payload["result_url"]=pre_assigned_url
        message_body=json.dumps(payload)
        print(payload["body"])
        response = self.sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageGroupId = message_deduplication_id,
            MessageDeduplicationId = message_deduplication_id
        )
        message_id = response['MessageId']
        download_url = self.generate_presigned_url(payload["config"]["aws_s3"]["storage_bucket"],object_key,"get_object")
        print(f"Sent message with ID: {message_id}, results uploaded in: {download_url}")

        self.wait_for_upload_head(payload["config"]["aws_s3"]["storage_bucket"],object_key)
        response = self.download_contents(payload["config"]["aws_s3"]["storage_bucket"],object_key)
        return response


    def wait_for_upload(self, bucket_name, object_key):
        waiter = self.s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=bucket_name, Key=object_key)

    def wait_for_upload_head(self,bucket_name, object_key, poll_interval=1):
        while True:
            try:
                response = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
                print("File uploaded successfully")
                break
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # print("Waiting for file to be uploaded...")
                    time.sleep(poll_interval)
                else:
                    print("An error occurred:", e)
                    break

    def download_file(self, presigned_url):
        response = requests.get(presigned_url)
        if response.status_code == 200:
            return response.content.decode('utf-8')
        return None

    def download_contents(self, bucket_name, object_key):
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            contents = json.loads(response['Body'].read().decode('utf-8'))
            return contents
        except Exception as e:
            print("Error downloading object:", str(e))


    def get_sqs_queue_arn(self,queue_url):

        response = self.sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        queue_arn = response['Attributes']['QueueArn']
        return queue_arn

    def get_queue_url(self,queue_name):

        response = self.sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        return queue_url

