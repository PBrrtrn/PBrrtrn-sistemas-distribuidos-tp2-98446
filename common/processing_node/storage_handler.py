import json
from os.path import exists
from abc import ABC, abstractmethod

# TODO: Cada nodo debe tener su propia carpeta para loggear y debe indexar por cliente
FILENAME = 'log'
COMMIT_CHAR = "C\n"


class StorageHandler(ABC):
    def __init__(self, storage_directory):
        filepath = f"{storage_directory}/{FILENAME}"
        self.__load_storage_from_disk(filepath)
        self.file = open(filepath, 'a+')

    def prepare(self, message: bytes):
        to_log = self._generate_log_map(message)
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def commit(self):
        self.file.write(COMMIT_CHAR)
        self.file.flush()

    def get_storage(self):
        return self.storage

    def __load_storage_from_disk(self, file_path):
        self.storage = {}
        if not exists(file_path):
            return
        with open(file_path, "r+") as file:
            for line in file:
                if line.endswith(COMMIT_CHAR):
                    log_map = json.loads(line[:-len(COMMIT_CHAR)])
                    self._update_memory_map_with_logs(log_map)
                elif not line.endswith('\n'):
                    file.write('\n')

    @abstractmethod
    def _update_memory_map_with_logs(self, log_map):
        pass

    @abstractmethod
    def _generate_log_map(self, message: bytes):
        pass

    def __write_log_line(self, to_log):
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
