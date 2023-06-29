import json

from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler
from common.rabbitmq.rpc_client import RPCClient

FILENAME = 'eof_sent_rpc'
COMMIT_CHAR = "C\n"


class RPCResponderOutputProcessor:
    def __init__(self, rpc_queue: Queue, storage_handler: StorageHandler,
                 optional_rpc_eof: RPCClient = None, optional_rpc_eof_byte: bytes = None):
        self.rpc_queue = rpc_queue
        self.storage_handler = storage_handler
        self.optional_rpc_eof = optional_rpc_eof
        self.optional_rpc_eof_byte = optional_rpc_eof_byte
        self.storage = {"id_last_message_responded": 0, "eofs_sent": 0, "rpc_eof_sent": False}
        filepath = f".eof/{FILENAME}"
        self.file = open(filepath, 'a+')

    def process_output(self, channel, message: bytes, method, properties):
        # if self.storage["id_last_message_responded"] == message.id: #Message id hay q cargarlo
        #    channel.basic_ack(delivery_tag=method.delivery_tag)
        self.prepare_send_message()
        self.rpc_queue.respond(
            message=message,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
        )
        self.commit()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self):
        if not self.storage["rpc_eof_sent"] and self.optional_rpc_eof is not None:
            print(f"Sending {self.optional_rpc_eof_byte}")
            self.prepare_send_rcp_eof()
            self.optional_rpc_eof.write_eof(self.optional_rpc_eof_byte)
            self.commit()

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

    def prepare_send_rcp_eof(self):
        to_log = self._generate_log_map_send_rpc_eof()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def _generate_log_map_send_rpc_eof(self):
        return {
            "id_last_message_responded": self.storage["id_last_message_responded"],
            "eofs_sent": self.storage["eofs_sent"],
            "rpc_eof_sent": True
        }

    def _generate_log_map_send_message(self):
        return {
            "id_last_message_responded": self.storage["id_last_message_responded"] + 1,
            "eofs_sent": self.storage["eofs_sent"],
            "rpc_eof_sent": self.storage["rpc_eof_sent"]
        }

    def _generate_log_map(self):
        return {
            "id_last_message_responded": self.storage["id_last_message_responded"],
            "eofs_sent": self.storage["eofs_sent"] + 1,
            "rpc_eof_sent": self.storage["rpc_eof_sent"]
        }
