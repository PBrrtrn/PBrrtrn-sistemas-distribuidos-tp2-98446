import json
from abc import ABC, abstractmethod
from os.path import exists
from common.processing_node.logs_reader import LogsReader

FILENAME = 'log'
COMMIT_CHAR = "C\n"
CHECKPOINT_BEGIN = "CHECKPOINT_START"
CHECKPOINT_END = "CHECKPOINT_END\n"
LOGS_READER_BUFFER_SIZE = 1024 * 8


class StorageHandler(ABC):
    def __init__(self, storage_directory, client_id='', filename=FILENAME, checkpoint_frequency=-1):
        self.checkpoint_frequency = checkpoint_frequency
        self.storage = {}
        self.commits = 0
        filepath = f"{storage_directory}/{filename}_{client_id}"
        self.__load_storage_from_disk(filepath)
        self.file = open(filepath, 'a+')

    def prepare(self, message):
        if self.commits == self.checkpoint_frequency:
            self.__write_checkpoint()
        to_log = self._generate_log_map(message)
        self._update_memory_map_with_logs(self.storage, to_log)
        self.__write_log_line(to_log)

    def commit(self):
        self.file.write(COMMIT_CHAR)
        self.file.flush()
        self.commits += 1

    def get_storage(self):
        return self.storage

    def __load_storage_from_disk(self, file_path):
        if exists(file_path):
            logs_reader = LogsReader(
                logs_path=file_path,
                buffer_size=LOGS_READER_BUFFER_SIZE,
                commit=COMMIT_CHAR,
                checkpoint_begin=CHECKPOINT_BEGIN,
                checkpoint_end=CHECKPOINT_END
            )
            self.storage, self.commits = logs_reader.load_storage(self._update_memory_map_with_logs)

    @abstractmethod
    def _generate_log_map(self, message: bytes):
        pass

    @abstractmethod
    def _update_memory_map_with_logs(self, storage, log_map):
        pass

    def __write_log_line(self, to_log):
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def __write_checkpoint(self):
        self.file.write(CHECKPOINT_BEGIN)
        checkpoint = self._create_checkpoint_from_storage()
        json.dump(checkpoint, self.file, indent=None)
        self.file.write(CHECKPOINT_END)
        self.file.flush()
        self.commits = 0

    def _create_checkpoint_from_storage(self):
        return self.storage

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
