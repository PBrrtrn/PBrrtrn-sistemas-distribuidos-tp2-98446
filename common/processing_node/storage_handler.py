from abc import ABC, abstractmethod

LOG_FILE_PATH = "path"
STORAGE_FILE_PATH = "storage_path"
AUX_STORAGE_FILE_PATH = "storage_path"
COMMIT_CHAR = "~"


class StorageHandler(ABC):
    def __init__(self):
        self.storage = {}

    def prepare(self, message: bytes):
        to_log = self._generate_log_map(message)
        # update memory map
        # flush log to file (operations can be interchangeably done, does not matter the order)
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    @abstractmethod
    def _generate_log_map(self, message: bytes):
        pass

    @abstractmethod
    def _update_memory_map_with_logs(self, log_map):
        pass

    def __write_log_line(self, to_log):
        pass
        # with open(LOG_FILE_PATH, "w") as file:
        #   json.dump(to_log, file) #Archivo ya abierto + flush

    def commit(self):
        pass
        # with open(LOG_FILE_PATH, "w") as file:
            # file.write(COMMIT_CHAR)  # Archivo ya abierto + file.flush

    def update_changes_in_disk(self):
        pass

    def get_storage(self):
        return self.storage

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
