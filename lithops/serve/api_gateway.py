
import boto3

class APIGateway:
    def __init__(self, config):
        self.api_name = config["api_gateway"]["api_name"]
        self.region_name = config["api_gateway"]["region_name"]
        self.api_gateway_role = config["api_gateway"]["api_gateway_role"]
        self.aws_session = boto3.Session(
            aws_access_key_id=config["aws"]['access_key_id'],
            aws_secret_access_key=config["aws"]['secret_access_key'],
            aws_session_token=config["aws"].get('session_token'),
            region_name=self.region_name
        )
        self.apigw_client = self.aws_session.client('apigateway')



    def is_api_gateway_exists(self, api_name):
        apis = self.apigw_client.get_rest_apis()['items']
        existing_api = next((api for api in apis if api['name'] == api_name), None)
        return existing_api is not None

    def create_api_gateway(self, lambda_function_name):
        # Check if the API Gateway already exists
        if self.is_api_gateway_exists(self.api_name):
            print(f"API Gateway '{self.api_name}' already exists.")
            return None

        api_response = self.apigw_client.create_rest_api(
            name=self.api_name,
            description='API Gateway to Lambda Orchestrator',
            endpointConfiguration = {
                'types': ['REGIONAL']
            }
        )
        rest_api_id = api_response['id']
        # Create a root resource
        root_resource = self.apigw_client.get_resources(restApiId=rest_api_id)['items'][0]
        root_resource_id = root_resource['id']

        # Create a child resource
        parent_resource_response = self.apigw_client.create_resource(
            restApiId=rest_api_id,
            parentId=root_resource_id,
            pathPart='off-sample'
        )
        parent_resource_id = parent_resource_response['id']

        child_resource_response = self.apigw_client.create_resource(
            restApiId=rest_api_id,
            parentId=parent_resource_id,
            pathPart='predict'
        )
        child_resource_id = child_resource_response['id']

        # Create a POST method under the resource
        self.apigw_client.put_method(
            restApiId=rest_api_id,
            resourceId=child_resource_id,
            httpMethod='POST',
            authorizationType='NONE'
        )

        # Set up the integration with the Lambda function
        lambda_uri = f'arn:aws:apigateway:{self.region_name}:lambda:path/2015-03-31/functions/arn:aws:lambda:{self.region_name}:{self.aws_session.client("sts").get_caller_identity().get("Account")}:function:{lambda_function_name}/invocations'
        self.apigw_client.put_integration(
            restApiId=rest_api_id,
            resourceId=child_resource_id,
            httpMethod='POST',
            type='AWS_PROXY',
            integrationHttpMethod='POST',
            uri=lambda_uri,
            credentials=self.api_gateway_role
        )

        # Add a method response
        self.apigw_client.put_method_response(
            restApiId=rest_api_id,
            resourceId=child_resource_id,
            httpMethod='POST',
            statusCode='200'
        )

        # Add an integration response
        self.apigw_client.put_integration_response(
            restApiId=rest_api_id,
            resourceId=child_resource_id,
            httpMethod='POST',
            statusCode='200',
            selectionPattern=''
        )

        # Deploy the API
        self.apigw_client.create_deployment(
            restApiId=rest_api_id,
            stageName='prod'
        )


        # Return the API Gateway URL
        api_url = f'https://{rest_api_id}.execute-api.{self.region_name}.amazonaws.com/prod'
        return api_url


    def delete_api_gateway(self):
        # Check if the API Gateway exists
        if not self.is_api_gateway_exists(self.api_name):
            print(f"API Gateway '{self.api_name}' does not exist.")
            return

        # Get the API Gateway ID
        apis = self.apigw_client.get_rest_apis()['items']
        api = next((api for api in apis if api['name'] == self.api_name), None)
        rest_api_id = api['id']

        # Delete the API Gateway
        self.apigw_client.delete_rest_api(restApiId=rest_api_id)
        print(f"API Gateway '{self.api_name}' deleted successfully.")

    def get_api_gateway_url(self):
        response = self.apigw_client.get_rest_apis()

        for api in response['items']:
            if api['name'] == self.api_name:
                api_gateway_id = api['id']
                break
        else:
            return None

        response = self.apigw_client.get_stages(
            restApiId=api_gateway_id
        )

        # Assuming you have only one stage, retrieve the first one
        stage_name = response['item'][0]['stageName']

        # Retrieve the URL of the API Gateway
        api_gateway_url = f"https://{api_gateway_id}.execute-api.{boto3.Session().region_name}.amazonaws.com/{stage_name}"

        return api_gateway_url

