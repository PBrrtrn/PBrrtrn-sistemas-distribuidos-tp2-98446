from common.logs_reader import LogsReader


def logs_reader_test():
    file_path = "log"
    logs_reader = LogsReader(
        logs_path=file_path,
        commit="C\n",
        checkpoint_begin="CHECKPOINT_START",
        checkpoint_end="CHECKPOINT_END",
        buffer_size=9
    )

    logs_reader.load_storage(foo)


def foo():
    pass


if __name__ == '__main__':
    logs_reader_test()
