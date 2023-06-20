class StorageOutputHandler:
    def __init__(self):
        pass

    def process_output(self, message: bytes):
        pass

    def finish_processing(self, node_id: str):
        pass
        # while True:
        #     request = self.rpc_server.serve()
        #     request_type = request[:common.network.constants.HEADER_TYPE_LEN]
        #     request_body = request[common.network.constants.HEADER_TYPE_LEN:]
        #
        #     request_result = self.request_processor.process_input(request_type, request_body)
