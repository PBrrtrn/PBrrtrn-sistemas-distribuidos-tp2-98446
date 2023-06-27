import json
from abc import ABC, abstractmethod
from os.path import exists

# TODO: Cada nodo debe tener su propia carpeta para loggear y debe indexar por cliente
FILENAME = 'log'
COMMIT_CHAR = "C\n"
CHECKPOINT_BEGIN = "CHECKPOINT_START"
CHECKPOINT_END = "CHECKPOINT_END\n"
CHECKPOINT_FREQUENCY = 10


class StorageHandler(ABC):
    def __init__(self, storage_directory):
        filepath = f"{storage_directory}/{FILENAME}"
        self.commits = 0  # TODO: Al caer cuenta desde cero - debería contar desde el último checkpoint
        self.__load_storage_from_disk(filepath)
        self.file = open(filepath, 'a+')

    def prepare(self, message: bytes):
        to_log = self._generate_log_map(message)
        self._update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)

    def commit(self):
        self.file.write(COMMIT_CHAR)
        self.file.flush()
        self.commits += 1

        if self.commits > CHECKPOINT_FREQUENCY:
            self.__write_checkpoint()

    def get_storage(self):
        return self.storage

    def __load_storage_from_disk(self, file_path):
        # TODO: Usar log_reader
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
    def _generate_log_map(self, message: bytes):
        pass

    @abstractmethod
    def _update_memory_map_with_logs(self, log_map):
        pass

    def __write_log_line(self, to_log):
        json.dump(to_log, self.file, indent=None)
        self.file.flush()

    def __write_checkpoint(self):
        self.file.write(CHECKPOINT_BEGIN)
        json.dump(self.storage, self.file, indent=None)
        self.file.write(CHECKPOINT_END)
        self.file.flush()

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
