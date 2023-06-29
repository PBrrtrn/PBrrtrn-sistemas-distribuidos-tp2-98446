import json

from common.rabbitmq.queue import Queue
from common.processing_node.storage_handler import StorageHandler

FILENAME = 'eof_sent_rpc'
COMMIT_CHAR = "C\n"

class RPCResponderOutputProcessor:
    def __init__(self, rpc_queue: Queue, storage_handler: StorageHandler):
        self.rpc_queue = rpc_queue
        self.storage_handler = storage_handler
        self.storage = {"id_last_message_responded": 0, "eofs_sent": 0}
        filepath = f".eof/{FILENAME}"
        self.file = open(filepath, 'a+')

    def process_output(self, channel, message: bytes, method, properties):
        #if self.storage["id_last_message_responded"] == message.id: #Message id hay q cargarlo
        #    channel.basic_ack(delivery_tag=method.delivery_tag)
        self.prepare_send_message()
        self.rpc_queue.respond(
            message=message,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
        )
        self.commit()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self, message: bytes, delivery_tag, correlation_id, reply_to):
        # Eventualmente tmb recibe el id del cliente

        #self.storage_handler.prepare_delete()
        #self.prepare()
        """self.rpc_queue.respond(
            message=message,
            to=reply_to,
            correlation_id=correlation_id,
        )"""
        #self.commit()
        #self.storage_handler.commit_delete()

    def prepare(self):
        to_log = self._generate_log_map()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def prepare_send_message(self):
        to_log = self._generate_log_map_send_message()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def commit(self):
        if self.file is None:
            return
        self.file.write(COMMIT_CHAR)
        self.file.flush()

    def __write_log_line(self, to_log):
        if self.file is None:
            return
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def _update_memory_map_with_logs(self, to_log):
        self.storage = to_log

    def _generate_log_map_send_message(self):
        return {
            "id_last_message_responded": self.storage["id_last_message_responded"] + 1,
            "eofs_sent": self.storage["eofs_sent"]
        }

    def _generate_log_map(self):
        return {
            "id_last_message_responded": self.storage["id_last_message_responded"],
            "eofs_sent": self.storage["eofs_sent"] + 1
        }
