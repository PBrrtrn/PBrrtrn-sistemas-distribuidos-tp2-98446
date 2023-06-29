import os


def setup_file(file):
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    if file_size != 0:
        file.seek(file_size - 1)
        last_character = file.read(1)
        if last_character != '\n':
            file.write('\n')


def read_lines_backwards(file, buffer_size):
    segment = None
    read_offset = 0
    file.seek(0, os.SEEK_END)
    file_size = bytes_remaining = file.tell()
    while bytes_remaining > 0:
        read_offset = min(file_size, read_offset + buffer_size)
        file.seek(file_size - read_offset)

        chunk = file.read(min(bytes_remaining, buffer_size))
        bytes_remaining -= buffer_size
        lines = chunk.split('\n')

        if segment is not None:
            if chunk[-1] != '\n':
                lines[-1] += segment
            else:
                position = file_size - read_offset + len(lines[0])
                yield segment + '\n', position

        segment = lines[0]
        for index in range(len(lines) - 1, 0, -1):
            if lines[index]:
                position = file_size - read_offset + sum(len(line) + 1 for line in lines[:index + 1])
                yield lines[index] + '\n', position

    if segment is not None:
        yield segment, 0
