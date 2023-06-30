from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler

FILENAME = 'eof_received'


class EOFHandler(StorageHandler):

    def register_eof(self, channel, method, _client_id):
        self.prepare({})
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.commit()

    def _generate_log_map(self, _message):
        return {
            "received_eof_signals": self.storage.get("received_eof_signals", 0) + 1,
        }

    def _update_memory_map_with_logs(self, storage, log_map):
        storage["received_eof_signals"] = log_map["received_eof_signals"]

    def number_of_received_eof_signals(self):
        return self.storage.get("received_eof_signals", 0)
