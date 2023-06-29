import json

from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.rpc_client import RPCClient
import common.network.constants

FILENAME = 'eof_sent'
COMMIT_CHAR = "C\n"


class ForwardingOutputProcessor:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter, output_eof: bytes,
                 optional_rpc_eof: RPCClient = None):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer
        self.output_eof = output_eof
        self.optional_rpc_eof = optional_rpc_eof
        self.storage = {"id_last_message_forwarded": 0, "eofs_sent": 0, "rpc_eof_sent": False}
        filepath = f".eof/{FILENAME}"
        self.file = open(filepath, 'a+')

    def process_output(self, channel, message: bytes, method, _properties):
        if message is None:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        # if self.storage["id_last_message_forwarded"] == message.id: #Message id hay q cargarlo
        #    channel.basic_ack(delivery_tag=method.delivery_tag)
        self.prepare_send_message()
        self.output_exchange_writer.write(message)
        self.commit()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self):
        if not self.storage["rpc_eof_sent"] and self.optional_rpc_eof is not None:
            self.prepare_send_rcp_eof()
            self.optional_rpc_eof.write_eof(self.output_eof)
            self.commit()
        remaining_eofs = self.n_output_peers - self.storage["eofs_sent"]
        for i in range(remaining_eofs):
            self.prepare()
            self.output_exchange_writer.write(self.output_eof)
            self.commit()

    def prepare(self):
        to_log = self._generate_log_map()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def _generate_log_map(self):
        return {
            "id_last_message_forwarded": self.storage["id_last_message_forwarded"],
            "eofs_sent": self.storage["eofs_sent"] + 1,
            "rpc_eof_sent": self.storage["rpc_eof_sent"]
        }

    def _update_memory_map_with_logs(self, to_log):
        self.storage = to_log

    def __write_log_line(self, to_log):
        if self.file is None:
            return
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def commit(self):
        if self.file is None:
            return
        self.file.write(COMMIT_CHAR)
        self.file.flush()

    def prepare_send_message(self):
        to_log = self._generate_log_map_send_message()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def _generate_log_map_send_message(self):
        return {
            "id_last_message_forwarded": self.storage["id_last_message_forwarded"] + 1,
            "eofs_sent": self.storage["eofs_sent"],
            "rpc_eof_sent": self.storage["rpc_eof_sent"]
        }

    def prepare_send_rcp_eof(self):
        to_log = self._generate_log_map_send_rpc_eof()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def _generate_log_map_send_rpc_eof(self):
        return {
            "id_last_message_forwarded": self.storage["id_last_message_forwarded"],
            "eofs_sent": self.storage["eofs_sent"],
            "rpc_eof_sent": True
        }
