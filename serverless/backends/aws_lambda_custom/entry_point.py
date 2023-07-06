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
import sys
import PIL
import zipfile
import shutil
from time import time

import boto3


def import_torch():
    if not os.path.exists('/tmp/python'):
        os.mkdir('/tmp/python')
    s3_client = boto3.client('s3')
    s3_client.download_file('off-sample', 'torch.zip', '/tmp/python/torch.zip')
    torch_dir = '/tmp/python/torch'
    # append the torch_dir to PATH so python can find it
    sys.path.append(torch_dir)
    python_dir = '/opt/python'
    sys.path.append(python_dir)
    if not os.path.exists(torch_dir):
        tempdir = '/tmp/python/_torch'
        if os.path.exists(tempdir):
            shutil.rmtree(tempdir)
        zipfile.ZipFile('/tmp/python/torch.zip', 'r').extractall(tempdir)
        os.rename(tempdir, torch_dir)


start = time()
import_torch()
end = time()
print("import torch time", end - start)
from lithops.version import __version__
from lithops.utils import setup_lithops_logger
from lithops.worker import function_handler
from lithops.worker import function_invoker
from lithops.worker.utils import get_runtime_metadata
from lithops.serverless.backends.aws_lambda_custom.custom_code.function import lambda_function

logger = logging.getLogger('lithops.worker')


def lambda_handler(event, context):
    os.environ['__LITHOPS_ACTIVATION_ID'] = context.aws_request_id
    os.environ['__LITHOPS_BACKEND'] = 'AWS Lambda'

    setup_lithops_logger(event.get('log_level', logging.INFO))
    print(event)
    if 'get_metadata' in event:
        logger.info(f"Lithops v{__version__} - Generating metadata")
        return get_runtime_metadata()
    elif (event['config']['lithops']['backend'] == 'aws_lambda_custom'):
        print('CUSTOM FUNCTION')
        result = lambda_function(event['data_byte_strs']['payload'])
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

