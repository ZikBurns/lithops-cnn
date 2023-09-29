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
import json
import sys
import PIL
import zipfile
import shutil
from time import time
from lithops.version import __version__
from lithops.utils import setup_lithops_logger
from lithops.worker import function_handler
from lithops.worker import function_invoker
from lithops.worker.utils import get_runtime_metadata
from lithops.serverless.backends.aws_lambda_custom_orchestrator.custom_code.function import lambda_function, \
    lambda_function_sqs, lambda_function_benchmark_batch_split_streaming

logger = logging.getLogger('lithops.worker')


def lambda_handler(event, context):
    os.environ['__LITHOPS_ACTIVATION_ID'] = context.aws_request_id
    os.environ['__LITHOPS_BACKEND'] = 'AWS Lambda'

    setup_lithops_logger(event.get('log_level', logging.INFO))

    if "resource" in event:
        event = json.loads(event["body"])
    elif "Records" in event:
        return lambda_function_sqs(event["Records"])
        event = json.loads(event["Records"][0]["body"])
        return lambda_function(event)

    print(event)
    if 'get_metadata' in event:
        logger.info(f"Lithops v{__version__} - Generating metadata")
        return get_runtime_metadata()
    elif (event['config']['lithops']['backend'] == 'aws_lambda_custom_orchestrator'):
        print('CUSTOM FUNCTION')
        result = lambda_function_benchmark_batch_split_streaming(event)
        return result

    if 'get_metadata' in event:
        logger.info(f"Lithops v{__version__} - Generating metadata")
        return get_runtime_metadata()
    elif 'remote_invoker' in event:
        logger.info(f"Lithops v{__version__} - Starting AWS Lambda invoker")
        function_invoker(event)
    else:
        logger.info(f"Lithops v{__version__} - Starting AWS Lambda execution")
        function_handler(event)

    return {"Execution": "Finished"}




