import json
import os
from typing import Callable


def _setup_file(file):
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(file_size - 1)
    last_character = file.read(1)
    if last_character != '\n':
        file.write('\n')


class LogsReader:
    BUFFER_SIZE = 8192

    def __init__(self, logs_path, checkpoint_begin, checkpoint_end, commit, buffer_size):
        self.logs_path = logs_path
        self.checkpoint_begin_char = checkpoint_begin
        self.checkpoint_end_char = checkpoint_end
        self.commit = commit
        self.buffer_size = buffer_size

    def load_storage(self, update_storage_callback: Callable):
        storage = {}
        commits_since_last_checkpoint = 0
        with open(self.logs_path, "r+") as file:
            _setup_file(file)
            for line, position in self._read_lines_backwards(file):
                if line.startswith(self.checkpoint_begin_char) and line.endswith(self.checkpoint_end_char):
                    raw_json = line[len(self.checkpoint_begin_char):-len(self.checkpoint_end_char)]
                    storage = json.loads(raw_json)
                    file.seek(position)
                    for log_line in file:
                        if log_line.endswith(self.commit):
                            commit = json.loads(log_line[:-len(self.commit)])
                            update_storage_callback(storage, commit)
                            commits_since_last_checkpoint += 1
                    return storage, commits_since_last_checkpoint

            file.seek(0)
            for line in file:
                if line.endswith(self.commit):
                    commit = json.loads(line[:-len(self.commit)])
                    update_storage_callback(storage, commit)
                    commits_since_last_checkpoint += 1

        return storage, commits_since_last_checkpoint

    def _read_lines_backwards(self, file):
        segment = None
        read_offset = 0
        file.seek(0, os.SEEK_END)
        file_size = bytes_remaining = file.tell()
        while bytes_remaining > 0:
            read_offset = min(file_size, read_offset + self.buffer_size)
            file.seek(file_size - read_offset)

            chunk = file.read(min(bytes_remaining, self.buffer_size))
            bytes_remaining -= self.buffer_size
            lines = chunk.split('\n')

            if segment is not None:
                if chunk[-1] != '\n':
                    lines[-1] += segment
                else:
                    position = file_size - read_offset + len(lines[0])
                    yield segment, position

            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if lines[index]:
                    position = file_size - read_offset + sum(len(line) + 1 for line in lines[:index + 1])
                    yield lines[index], position

        if segment is not None:
            yield segment, 0
