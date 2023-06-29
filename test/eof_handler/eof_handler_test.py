import os
import unittest
from unittest.mock import Mock

from common.processing_node.queue_consumer.eof_handler import EOFHandler

LOGS_DIR = "eof_handler_logs"
LOGS_FILENAME = "eof_received"


class EOFHandlerTest(unittest.TestCase):
    def setUp(self):
        file_path = f"{LOGS_DIR}/{LOGS_FILENAME}"
        if os.path.exists(file_path):
            os.remove(file_path)

    def test_log_eof(self):
        mock_channel = Mock()
        mock_channel.basic_ack()
        mock_method = Mock()
        _ = mock_method.delivery_tag

        eof_handler = EOFHandler(eof_directory=LOGS_DIR)
        eof_handler.two_phase_commit(mock_channel, mock_method)

        with open(f"{LOGS_DIR}/{LOGS_FILENAME}", 'r') as file:
            line = file.readline()
            self.assertEqual("{\"received_eof_signals\": 1}C\n", line)

    def test_recover_eofs(self):
        with open(f"{LOGS_DIR}/{LOGS_FILENAME}", 'w+') as file:
            file.write("{\"received_eof_signals\": 1}C\n")
            file.write("{\"received_eof_signals\": 2}C\n")

        eof_handler = EOFHandler(eof_directory=LOGS_DIR)
        self.assertEqual(2, eof_handler.number_of_received_eof_signals())

    def test_recover_eofs_with_broken_commits(self):
        with open(f"{LOGS_DIR}/{LOGS_FILENAME}", 'w+') as file:
            file.write("{\"received_eof_signals\": 1}C\n")
            file.write("{\"received_eof_signals\": 2}C\n")
            file.write("{\"received_eof_s")

        eof_handler = EOFHandler(eof_directory=LOGS_DIR)
        self.assertEqual(2, eof_handler.number_of_received_eof_signals())
