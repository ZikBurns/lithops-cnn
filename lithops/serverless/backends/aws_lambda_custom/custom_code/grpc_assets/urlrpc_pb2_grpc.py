# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from . import urlrpc_pb2 as urlrpc__pb2


class URLRPCStub(object):
    """to compile
    python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. --pyi_out=. ./urlrpc.proto

    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Add = channel.unary_unary(
                '/urlrpc.URLRPC/Add',
                request_serializer=urlrpc__pb2.urlRequest.SerializeToString,
                response_deserializer=urlrpc__pb2.urlResponse.FromString,
                )


class URLRPCServicer(object):
    """to compile
    python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. --pyi_out=. ./urlrpc.proto

    """

    def Add(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_URLRPCServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Add': grpc.unary_unary_rpc_method_handler(
                    servicer.Add,
                    request_deserializer=urlrpc__pb2.urlRequest.FromString,
                    response_serializer=urlrpc__pb2.urlResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'urlrpc.URLRPC', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class URLRPC(object):
    """to compile
    python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. --pyi_out=. ./urlrpc.proto

    """

    @staticmethod
    def Add(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/urlrpc.URLRPC/Add',
            urlrpc__pb2.urlRequest.SerializeToString,
            urlrpc__pb2.urlResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)