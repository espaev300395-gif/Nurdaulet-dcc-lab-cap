import grpc
import kvstore_pb2

class KeyValueStoreServicer(object):
    def Get(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Put(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

def add_KeyValueStoreServicer_to_server(servicer, server):
    rpc_method_handlers = {
        'Get': grpc.unary_unary_rpc_method_handler(
            servicer.Get,
            request_deserializer=kvstore_pb2.GetRequest.FromString,
            response_serializer=kvstore_pb2.GetReply.SerializeToString,
        ),
        'Put': grpc.unary_unary_rpc_method_handler(
            servicer.Put,
            request_deserializer=kvstore_pb2.PutRequest.FromString,
            response_serializer=kvstore_pb2.PutReply.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        'kvstore.KeyValueStore', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))

class KeyValueStoreStub(object):
    def __init__(self, channel):
        self.Get = channel.unary_unary(
            '/kvstore.KeyValueStore/Get',
            request_serializer=kvstore_pb2.GetRequest.SerializeToString,
            response_deserializer=kvstore_pb2.GetReply.FromString,
        )
        self.Put = channel.unary_unary(
            '/kvstore.KeyValueStore/Put',
            request_serializer=kvstore_pb2.PutRequest.SerializeToString,
            response_deserializer=kvstore_pb2.PutReply.FromString,
        )
