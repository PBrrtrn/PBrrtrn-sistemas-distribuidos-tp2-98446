import json
from abc import ABC, abstractmethod


LOGS_DIR = "./storage-handler-test/logs"
LOG_FILENAME = "log"
# TODO: Cada nodo debe tener su propia carpeta para loggear y debe indexar por cliente
COMMIT_CHAR = "C\n"


class StorageHandler(ABC):
    def __init__(self):
        self.file = open(f"{LOGS_DIR}/{LOG_FILENAME}", 'a+')
        self.storage = {}

    def prepare(self, message: bytes):
        to_log = self._generate_log_map(message)
        print(f"DEBUG - to_log: {to_log}")
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def commit(self):
        self.file.write(COMMIT_CHAR)
        self.file.flush()

    def get_storage(self):
        return self.storage

    @abstractmethod
    def _generate_log_map(self, message: bytes):
        pass

    @abstractmethod
    def _update_memory_map_with_logs(self, log_map):
        pass

    def __write_log_line(self, to_log):
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def update_changes_in_disk(self):
        pass

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
