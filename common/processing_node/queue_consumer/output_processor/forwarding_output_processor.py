import json
import os

from common.rabbitmq.exchange_writer import ExchangeWriter


FILENAME = 'eof_sent'
COMMIT_CHAR = "C\n"

class ForwardingOutputProcessor:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter, output_eof: bytes):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer
        self.output_eof = output_eof
        self.storage = {"eofs_sent": 0}
        filepath = f".eof/{FILENAME}"
        self.file = open(filepath, 'a+')

    def process_output(self, channel, message: bytes, method, _properties):
        if message is not None:
            self.output_exchange_writer.write(message)
        #Commit de que se escribió el mensaje
        #ACK
        channel.basic_ack(delivery_tag=method.delivery_tag)


    def finish_processing(self, _result, _delivery_tag, _correlation_id, _reply_to):
        remaining_eofs = self.n_output_peers - self.storage["eofs_sent"]
        for i in range(remaining_eofs):
            self.prepare()
            self.output_exchange_writer.write(self.output_eof)
            self.commit()
        # channel.basic_ack(delivery_tag=method.delivery_tag) No se debería hacer un ACK acá, no?

    def prepare(self):
        to_log = self._generate_log_map()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def _generate_log_map(self):
        return {
            "eofs_sent": self.storage["eofs_sent"] + 1
        }

    def _update_memory_map_with_logs(self, to_log):
        self.eofs_sent = to_log

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

