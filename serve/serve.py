import json
from typing import Optional, List, Union, Tuple, Dict, Any

from lithops import FunctionExecutor
from requests import post


class LithopsServe:
    def __init__(
            self,
            clean: bool = False,
            config: Optional[Dict[str, Any]] = None,
            runtime_memory: Optional[int] = None,
            max_workers: Optional[int] = None,
    ):
        self.fexec = FunctionExecutor(reset=clean,config=config, runtime_memory=runtime_memory,max_workers=max_workers)


    def run_orchestrator(
            self,
            data: Union[List[Any], Tuple[Any, ...], Dict[str, Any]],
            extra_env: Optional[Dict] = None,
            runtime_memory: Optional[int] = None,
            timeout: Optional[int] = None,
    ):
        return self.fexec.call_async_cnn_asyncio_orchestrator(data=data,extra_env=extra_env,runtime_memory=runtime_memory,timeout=timeout)

    def run_orchestrator_apigateway(
            self,
            data: Union[List[Any], Tuple[Any, ...], Dict[str, Any]],
            extra_env: Optional[Dict] = None,
            runtime_memory: Optional[int] = None,
            timeout: Optional[int] = None,
    ):
        return self.fexec.call_async_cnn_apigateway(data=data,extra_env=extra_env,runtime_memory=runtime_memory,timeout=timeout)

    def run_orchestrator_flask(self, api_url, doc):
        resp = post(url=api_url + "/off-sample/predict", json=json.dumps(doc), timeout=120)
        return resp.json()

    def enqueue(self,
            data: Union[List[Any], Tuple[Any, ...], Dict[str, Any]],
            extra_env: Optional[Dict] = None,
            runtime_memory: Optional[int] = None,
            timeout: Optional[int] = None,
    ):
        return self.fexec.call_async_cnn_sqs(data=data,extra_env=extra_env,runtime_memory=runtime_memory,timeout=timeout)

    def enqueue_multiple(self,
                         data: Union[List[Any], Tuple[Any, ...], Dict[str, Any]],
                         extra_env: Optional[Dict] = None,
                         runtime_memory: Optional[int] = None,
                         timeout: Optional[int] = None,
    ):
        return self.fexec.map_cnn_sqs(map_iterdata=data, extra_env=extra_env, runtime_memory=runtime_memory, timeout=timeout)
