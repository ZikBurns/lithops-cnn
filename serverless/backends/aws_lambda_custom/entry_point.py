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


def import_torch():
    torch_dir = '/tmp/python/torch'
    sys.path.append(torch_dir)
    python_dir = '/opt/python'
    sys.path.append(python_dir)
    if not os.path.exists(torch_dir):
        tempdir = '/tmp/python/_torch'
        if os.path.exists(tempdir):
            shutil.rmtree(tempdir)
        zipfile.ZipFile('/opt/python/torch.zip', 'r').extractall(tempdir)
        os.rename(tempdir, torch_dir)


import_torch()
from lithops.version import __version__
from lithops.utils import setup_lithops_logger
from lithops.worker import function_handler
from lithops.worker.handler import function_handler_custom
from lithops.worker import function_invoker
from lithops.worker.utils import get_runtime_metadata

logger = logging.getLogger('lithops.worker')


def lambda_handler(event, context):
    os.environ['__LITHOPS_ACTIVATION_ID'] = context.aws_request_id
    os.environ['__LITHOPS_BACKEND'] = 'AWS Lambda'

    setup_lithops_logger(event.get('log_level', logging.INFO))
    if (event['config']['lithops']['backend'] == 'aws_lambda_custom'):
        logger.info(f"Lithops v{__version__} - Starting CUSTOM AWS Lambda execution")
        result = function_handler_custom(event)
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
