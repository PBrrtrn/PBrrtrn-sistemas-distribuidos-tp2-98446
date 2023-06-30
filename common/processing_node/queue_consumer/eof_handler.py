from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler

FILENAME = 'eof_received'


class EOFHandler(StorageHandler):

    def register_eof(self, channel, method, message_id):
        self.prepare({"eof_message_id": message_id})
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.commit()

    def _generate_log_map(self, message):
        return {
            "received_eof_signals": self.storage.get("received_eof_signals", 0) + 1,
            "eof_message_id": message["eof_message_id"]
        }

    def _update_memory_map_with_logs(self, storage, log_map):
        storage["received_eof_signals"] = log_map["received_eof_signals"]
        storage["eof_message_id"] = log_map["eof_message_id"]

    def number_of_received_eof_signals(self):
        return self.storage.get("received_eof_signals", 0)

    def get_last_message_id(self):
        return self.storage["eof_message_id"]
