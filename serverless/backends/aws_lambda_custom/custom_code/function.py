
import json
from resources import PredictResource

def lambda_function(payload,batch_size=32):
    if isinstance(payload, str):
        payload = json.loads(payload)
    payload=payload["body"]
    predictions =  PredictResource("/tmp/torchscript_model.pt", batch_size).execute_inference(payload)
    return predictions