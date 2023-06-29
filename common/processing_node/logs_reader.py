import json
from typing import Callable

import common.read_lines_backwards


class LogsReader:
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
            common.read_lines_backwards.setup_file(file)
            for line, position in common.read_lines_backwards.read_lines_backwards(file, self.buffer_size):
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
