from common.processing_node.logs_reader import LogsReader
import unittest


class LogsReaderTest(unittest.TestCase):
    COMMIT = "C\n"
    BEGIN = "CHECKPOINT_START"
    END = "CHECKPOINT_END\n"

    def _buffer_test(self, buffer_size):
        file_path = "logs_reader_broken_checkpoint_log"
        logs_reader = LogsReader(
            logs_path=file_path,
            commit=self.COMMIT,
            checkpoint_begin=self.BEGIN,
            checkpoint_end=self.END,
            buffer_size=buffer_size
        )

        expected_storage = {'a': 4, 'b': 2, 'c': 3, 'd': 3}
        expected_commits = 4
        storage, commits = logs_reader.load_storage(update_storage_with_log_entry)
        self.assertEqual(expected_storage, storage)
        self.assertEqual(expected_commits, commits)

    def test_small_buffer(self):
        self._buffer_test(2)

    def test_medium_sized_buffer(self):
        self._buffer_test(64)

    def test_buffer_larger_than_file(self):
        self._buffer_test(1024)

    def test_buffer_fits_line_exactly(self):
        self._buffer_test(21)

    def test_no_checkpoints(self):
        file_path = "logs_reader_no_checkpoint_log"
        logs_reader = LogsReader(
            logs_path=file_path,
            commit=self.COMMIT,
            checkpoint_begin=self.BEGIN,
            checkpoint_end=self.END,
            buffer_size=64
        )

        expected_storage = {'a': 3, 'b': 1, 'c': 2}
        expected_commits = 4
        storage, commits = logs_reader.load_storage(update_storage_with_log_entry)
        self.assertEqual(expected_storage, storage)
        self.assertEqual(expected_commits, commits)

    def test_last_line_is_checkpoint(self):
        file_path = "logs_reader_last_line_is_checkpoint_log"
        logs_reader = LogsReader(
            logs_path=file_path,
            commit=self.COMMIT,
            checkpoint_begin=self.BEGIN,
            checkpoint_end=self.END,
            buffer_size=64
        )

        expected_storage = {'a': 3, 'b': 1, 'c': 2}
        expected_commits = 0
        storage, commits = logs_reader.load_storage(update_storage_with_log_entry)
        self.assertEqual(expected_storage, storage)
        self.assertEqual(expected_commits, commits)

    def test_last_line_is_broken_commit(self):
        file_path = "logs_reader_broken_commit_log"
        logs_reader = LogsReader(
            logs_path=file_path,
            commit=self.COMMIT,
            checkpoint_begin=self.BEGIN,
            checkpoint_end=self.END,
            buffer_size=64
        )

        expected_storage = {'a': 3, 'b': 1}
        expected_commits = 3
        storage, commits = logs_reader.load_storage(update_storage_with_log_entry)
        self.assertEqual(expected_storage, storage)
        self.assertEqual(expected_commits, commits)


def update_storage_with_log_entry(storage, log_entry):
    for key, value in log_entry.items():
        storage[key] = value


if __name__ == '__main__':
    unittest.main()
