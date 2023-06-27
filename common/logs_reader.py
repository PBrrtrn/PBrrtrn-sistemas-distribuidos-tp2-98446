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

    def load_storage(self, write_callback: Callable):
        commits_since_last_checkpoint = 0
        with open(self.logs_path, "r+") as file:
            storage = self._load_last_checkpoint(file)
            print(f"STORAGE: {storage}")
            next(file)
            for line in file:
                print(f"NEXT LINE: {line}")
                # Ir reconstruyendo el storage a partir del checkpoint
                pass

        return storage, commits_since_last_checkpoint

    def _load_last_checkpoint(self, file):
        _setup_file(file)
        for line in self._read_last_line(file):
            if line == -1:
                break

            if line.startswith(self.checkpoint_begin_char) and line.endswith(self.checkpoint_end_char):
                checkpoint_json = line[len(self.checkpoint_begin_char):-len(self.checkpoint_end_char)]
                return json.loads(checkpoint_json)

        return {}

    def _read_last_line(self, file):
        segment = None
        offset = 0
        file.seek(0, os.SEEK_END)
        file_size = remaining_size = file.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + self.buffer_size)
            file.seek(file_size - offset)
            buffer = file.read(min(remaining_size, self.buffer_size))
            remaining_size -= self.buffer_size
            lines = buffer.split('\n')
            # The first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # If the previous chunk starts right from the beginning of line
                # do not concat the segment to the last line of new chunk.
                # Instead, yield the segment first
                if buffer[-1] != '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if lines[index]:
                    yield lines[index]
        # Don't yield None if the file was empty
        yield -1

    # def _read_last_line(self, file):
    #     remaining = file.tell()
    #     read_start = max(0, remaining - self.buffer_size)
    #     read_size = min(remaining - read_start, self.buffer_size)
    #     while read_size > 0:
    #         chunk, remaining, read_size, read_start = self._read_chunk(file, read_start, read_size, remaining)
    #         split_chunk = chunk.split('\n')
    #         print(f"Split chunk: {split_chunk}")
    #
    #         if chunk == "asd":
    #             return chunk
    #
    #     return -1

    # def _read_chunk(self, file, read_start, bytes_to_read, bytes_remaining):
    #     file.seek(read_start)
    #     chunk = file.read(bytes_to_read)
    #
    #     bytes_remaining -= bytes_to_read
    #     bytes_to_read = min(bytes_remaining, self.buffer_size)
    #     read_start -= bytes_to_read
    #
    #     return chunk, bytes_remaining, bytes_to_read, read_start

