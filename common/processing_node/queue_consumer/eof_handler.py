import json
import os

import common.read_lines_backwards

FILENAME = 'eof_received'


class EOFHandler:
    BUFFER_SIZE = 64
    COMMIT_CHAR = "C\n"

    def __init__(self, eof_directory, append=''):
        filepath = f"{eof_directory}/{FILENAME}{append}"
        self.__load_storage_from_disk(file_path=filepath)
        self.file = None
        if os.path.isdir(eof_directory):
            self.file = open(filepath, 'a+')

    def __load_storage_from_disk(self, file_path):
        self.storage = {'received_eof_signals': 0}
        if os.path.exists(file_path):
            with open(file_path, 'r+') as file:
                common.read_lines_backwards.setup_file(file)
                for line, position in common.read_lines_backwards.read_lines_backwards(file, self.BUFFER_SIZE):
                    if line.endswith(self.COMMIT_CHAR):
                        self.storage = json.loads(line[:-len(self.COMMIT_CHAR)])
                        break
                    file.seek(position)

    def __prepare(self):
        to_log = self._generate_log_map()
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def __commit(self):
        if self.file is None:
            return
        self.file.write(self.COMMIT_CHAR)
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
