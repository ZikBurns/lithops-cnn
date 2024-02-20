#
# Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import logging
from lithops.version import __version__
from lithops.utils import setup_lithops_logger
from lithops.worker import function_handler
from lithops.worker import function_invoker
from lithops.worker.utils import get_runtime_metadata
from lithops.serverless.backends.aws_lambda_default.custom_code.function import lambda_function
from lithops.storage.utils import create_output_key
from lithops.storage import InternalStorage
from lithops.config import extract_storage_config
import time
import json
logger = logging.getLogger('lithops.worker')


def lambda_handler(event, context):
    print(event)
    start = time.time()
    os.environ['__LITHOPS_ACTIVATION_ID'] = context.aws_request_id
    os.environ['__LITHOPS_BACKEND'] = 'AWS Lambda'

    setup_lithops_logger(event.get('log_level', logging.INFO))

    if 'get_metadata' in event:
        logger.info(f"Lithops v{__version__} - Generating metadata")
        return get_runtime_metadata()
    elif (event['config']['lithops']['backend'] == 'aws_lambda_default'):
        print('CUSTOM FUNCTION')
        try:
            result = lambda_function(event['data_byte_strs']['payload'], event['config']['aws_s3']['storage_bucket'])
        except Exception as e:
            result =  {
                'statusCode': 500,
                'body': {'error': str(e)}
            }
        finally:
            result_body = json.dumps(result).encode('utf-8')
            output_key = create_output_key(event['executor_id'], event['job_key'], event['call_id'])
            storage_config = extract_storage_config(event['config'])
            internal_storage = InternalStorage(storage_config)
            internal_storage.put_data(output_key, result_body)
            end = time.time()
            print("Function inside handler took", end - start, "seconds")
            return result
    elif 'remote_invoker' in event:
        logger.info(f"Lithops v{__version__} - Starting AWS Lambda invoker")
        function_invoker(event)
    else:
        logger.info(f"Lithops v{__version__} - Starting AWS Lambda execution")
        function_handler(event)

    return {"Execution": "Finished"}
