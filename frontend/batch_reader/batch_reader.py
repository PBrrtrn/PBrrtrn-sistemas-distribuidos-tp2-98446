from typing import Callable

import csv


class BatchReader:
    def __init__(self, file_path: str, batch_size: int, line_validator: Callable, line_parser: Callable):
        self.batch_size = batch_size
        self.file_pointer = open(file_path)
        self.reader = csv.reader(self.file_pointer)
        self.line_validator = line_validator
        self.line_parser = line_parser

        self.__skip_header()

    def read(self):
        if self.file_pointer is None:
            raise 'Uninitialized file pointer'

        while True:
            try:
                batch = []
                for _ in range(self.batch_size):
                    line = next(self.reader)
                    try:
                        if self.line_validator(line):
                            batch.append(self.line_parser(line))
                    except ValueError:
                        continue
                yield batch
            except StopIteration:
                break

    def close(self):
        if self.file_pointer is None:
            raise 'Uninitialized file pointer'

        self.file_pointer.close()

    def __skip_header(self):
        next(self.reader)
