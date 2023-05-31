
import json
from lithops.serverless.backends.aws_lambda_custom.custom_code.resources import PredictResource

def lambda_function(payload):
    payload = payload["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    predictions = PredictResource("/opt/torchscript_model.pt").execute_inference(payload)
    result = {'predictions': predictions['predictions']}
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }