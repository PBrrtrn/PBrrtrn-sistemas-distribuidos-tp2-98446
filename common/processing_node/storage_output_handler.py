import common.network.constants
from common.rabbitmq.rpc_server import RPCServer


class StorageOutputHandler:
    def __init__(self, rpc_server: RPCServer, request_processor):
        self.storage = {}  # TODO: A disco
        self.rpc_server = rpc_server
        self.request_processor = request_processor

    def output_message(self, message: bytes):
        self.storage['a'] = message

    def output_eof(self, node_id: str):
        while True:
            request = self.rpc_server.serve()
            request_type = request[:common.network.constants.HEADER_TYPE_LEN]
            request_body = request[common.network.constants.HEADER_TYPE_LEN:]

            request_result = self.request_processor.process(request_type, request_body)
