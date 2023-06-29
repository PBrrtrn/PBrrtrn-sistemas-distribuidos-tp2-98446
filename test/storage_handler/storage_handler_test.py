import os
import pickle
import unittest

from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler

LOGS_DIR = "storage_handler_logs"
LOG_FILENAME = "log"


class TestStorageHandler(StorageHandler):
    def _generate_log_map(self, message: bytes):
        return pickle.loads(message)

    def _update_memory_map_with_logs(self, storage, log_map):
        for (key, value) in log_map.items():
            storage[key] = value


class StorageHandlerTest(unittest.TestCase):
    def setUp(self):
        file_path = f"{LOGS_DIR}/{LOG_FILENAME}"
        if os.path.exists(file_path):
            os.remove(file_path)

    def test_recovery_with_broken_commit_at_the_end(self):
        storage_handler = TestStorageHandler(storage_directory=LOGS_DIR, checkpoint_frequency=100)
        for i in range(1, 10):
            storage_handler.prepare(pickle.dumps({'a': i*2, 'b': i}))
            storage_handler.commit()

        storage_handler.prepare(pickle.dumps({'c': 2}))

        recovered_storage_handler = TestStorageHandler(storage_directory=LOGS_DIR, checkpoint_frequency=100)
        expected_recovered_storage = {'a': 18, 'b': 9}
        recovered_storage = recovered_storage_handler.get_storage()
        self.assertEqual(expected_recovered_storage, recovered_storage)

    def test_checkpoint(self):
        storage_handler = TestStorageHandler(storage_directory=LOGS_DIR, checkpoint_frequency=5)
        for i in range(1, 12):
            storage_handler.prepare(pickle.dumps({'a': i*2, 'b': i}))
            storage_handler.commit()

        with open(f"{LOGS_DIR}/{LOG_FILENAME}") as file:
            file_lines = file.readlines()
            self.assertEqual("CHECKPOINT_START{\"a\": 10, \"b\": 5}CHECKPOINT_END\n", file_lines[5])
            self.assertEqual("CHECKPOINT_START{\"a\": 20, \"b\": 10}CHECKPOINT_END\n", file_lines[11])

    def test_checkpoint_recovery(self):
        storage_handler = TestStorageHandler(storage_directory=LOGS_DIR, checkpoint_frequency=5)
        for i in range(1, 12):
            storage_handler.prepare(pickle.dumps({'a': i * 2, 'b': i}))
            storage_handler.commit()

        recovered_storage_handler = TestStorageHandler(storage_directory=LOGS_DIR, checkpoint_frequency=5)
        self.assertEqual({'a': 22, 'b': 11}, recovered_storage_handler.get_storage())