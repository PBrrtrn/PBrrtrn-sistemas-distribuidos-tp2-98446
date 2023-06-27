import json
import os
import pickle
from os.path import exists

FILENAME = 'eof_received'
COMMIT_CHAR = "C\n"


class EOFHandler:
    def __init__(self, eof_directory, append=''):
        filepath = f"{eof_directory}/{FILENAME}{append}"
        self.__load_storage_from_disk(file_path=filepath)
        self.file = None
        if os.path.isdir(eof_directory):
            self.file = open(filepath, 'a+')

    def __load_storage_from_disk(self, file_path):
        self.storage = {
            "received_eof_signals": 0,
            "last_result": b'',
            "last_delivery_tag": None,
            "last_correlation_id": None,
            "last_reply_to": None
        }
        """if not exists(file_path):
            return
        with open(file_path, "r+") as file:
            for line in file:
                if line.endswith(COMMIT_CHAR):
                    log_map = json.loads(line[:-len(COMMIT_CHAR)])
                    self._update_memory_map_with_logs(log_map)
                elif not line.endswith('\n'):
                    file.write('\n')"""

    def __prepare(self, result, method, properties):
        to_log = self._generate_log_map(result, method, properties)
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def __commit(self):
        if self.file is None:
            return
        self.file.write(COMMIT_CHAR)
        self.file.flush()

    def two_phase_commit(self, channel, result, method, properties):
        self.__prepare(result, method, properties)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.__commit()

    def _update_memory_map_with_logs(self, to_log):
        self.storage = to_log

    def get_last_result(self):
        last_result = self.storage["last_result"]
        if last_result is not None:
            last_result = pickle.dumps(self.storage["last_result"])
        return last_result, self.storage["last_delivery_tag"], \
               self.storage["last_correlation_id"], self.storage["last_reply_to"]

    def __write_log_line(self, to_log):
        if self.file is None:
            return
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def _generate_log_map(self, result, method, properties):
        try:
            result_to_save = pickle.loads(result)
        except Exception as _e:
            result_to_save = None
        return {
            "received_eof_signals": self.storage["received_eof_signals"] + 1,
            "last_result": result_to_save,
            "delivery_tag": method.delivery_tag,
            "correlation_id": properties.correlation_id,
            "reply_to": properties.reply_to
        }

    def number_of_received_eof_signals(self):
        return self.storage["received_eof_signals"]
