import os
import unittest

from common.processing_node.queue_consumer.forwarding_state_storage_handler import ForwardingStateStorageHandler

LOGS_DIR = "storage_handler_logs"
LOG_FILENAME = "eof_sent"


class ForwardingStateStorageHandlerTest(unittest.TestCase):
    def setUp(self):
        file_path = f"{LOGS_DIR}/{LOG_FILENAME}"
        if os.path.exists(file_path):
            os.remove(file_path)

    def test_incremental_message_ids(self):
        storage_handler = ForwardingStateStorageHandler(storage_directory=LOGS_DIR, filename=LOG_FILENAME)

        for _ in range(5):
            storage_handler.prepare_last_message_id_increment()
            storage_handler.commit()

        storage = storage_handler.get_storage()
        self.assertEqual(5, storage['id_last_message_forwarded'])

    def test_recover_incremental_message_ids(self):
        storage_handler = ForwardingStateStorageHandler(storage_directory=LOGS_DIR, filename=LOG_FILENAME)
        for _ in range(5):
            storage_handler.prepare_last_message_id_increment()
            storage_handler.commit()
        storage_handler.prepare_last_message_id_increment()

        recovered_storage_handler = ForwardingStateStorageHandler(storage_directory=LOGS_DIR, filename=LOG_FILENAME)
        recovered_storage = recovered_storage_handler.get_storage()
        self.assertEqual(5, recovered_storage['id_last_message_forwarded'])


if __name__ == '__main__':
    unittest.main()
