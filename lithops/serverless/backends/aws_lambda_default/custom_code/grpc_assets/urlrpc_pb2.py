# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: urlrpc.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0curlrpc.proto\x12\x06urlrpc\",\n\nurlRequest\x12\x1e\n\x08\x66inished\x18\x01 \x03(\x0b\x32\x0c.urlrpc.Dict\"\x1b\n\x0burlResponse\x12\x0c\n\x04urls\x18\x01 \x03(\t\"\"\n\x04\x44ict\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t28\n\x06URLRPC\x12.\n\x03\x41\x64\x64\x12\x12.urlrpc.urlRequest\x1a\x13.urlrpc.urlResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'urlrpc_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_URLREQUEST']._serialized_start=24
  _globals['_URLREQUEST']._serialized_end=68
  _globals['_URLRESPONSE']._serialized_start=70
  _globals['_URLRESPONSE']._serialized_end=97
  _globals['_DICT']._serialized_start=99
  _globals['_DICT']._serialized_end=133
  _globals['_URLRPC']._serialized_start=135
  _globals['_URLRPC']._serialized_end=191
# @@protoc_insertion_point(module_scope)
