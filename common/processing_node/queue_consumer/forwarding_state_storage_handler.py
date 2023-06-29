import pickle

from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler


class ForwardingStateStorageHandler(StorageHandler):

    def prepare_last_message_id_increment(self):
        self.prepare({
            "id_last_message_forwarded": self.storage.get("id_last_message_forwarded", 0) + 1,
            "eofs_sent": self.storage.get("eofs_sent", 0),
            "rpc_eof_sent": self.storage.get("rpc_eof_sent", False)
        })

    def prepare_set_rpc_eof_as_sent(self):
        self.prepare({
            "id_last_message_forwarded": self.storage.get("id_last_message_forwarded", 0),
            "eofs_sent": self.storage.get("eofs_sent", 0),
            "rpc_eof_sent": True
        })

    def prepare_eofs_sent_increment(self):
        self.prepare({
            "id_last_message_forwarded": self.storage.get("id_last_message_forwarded", 0),
            "eofs_sent": self.storage.get("eofs_sent", 0) + 1,
            "rpc_eof_sent": self.storage.get("rpc_eof_sent", False)
        })

    def _generate_log_map(self, message):
        return message

    def _update_memory_map_with_logs(self, storage, log_map):
        storage = log_map
