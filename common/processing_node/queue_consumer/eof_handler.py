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

    def __prepare(self):
        to_log = self._generate_log_map()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def __commit(self):
        if self.file is None:
            return
        self.file.write(COMMIT_CHAR)
        self.file.flush()

    def two_phase_commit(self, channel, method):
        self.__prepare()
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.__commit()

    def _update_memory_map_with_logs(self, to_log):
        self.storage = to_log

    def __write_log_line(self, to_log):
        if self.file is None:
            return
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def _generate_log_map(self):
        return {
            "received_eof_signals": self.storage["received_eof_signals"] + 1,
        }

    def number_of_received_eof_signals(self):
        return self.storage["received_eof_signals"]
