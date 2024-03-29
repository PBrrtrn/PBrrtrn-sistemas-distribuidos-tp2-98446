import unittest
import os
import pickle

from backend.by_year_and_station_trips_count.station_counter_storage_handler import StationCounterStorageHandler
from common.model.trip import Trip

N_BATCHES = 4
N_TRIPS_PER_BATCH = 2

LOGS_DIR = "storage_handler_logs"
LOG_FILENAME = "log"


class StationCounterStorageHandlerTest(unittest.TestCase):
    def setUp(self):
        file_path = f"{LOGS_DIR}/{LOG_FILENAME}"
        if os.path.exists(file_path):
            os.remove(file_path)

    def test_recovery_with_broken_commit_at_the_end(self):
        storage_handler = StationCounterStorageHandler(storage_directory=LOGS_DIR, checkpoint_frequency=100)
        cities = ['Vermont', 'Chicago', 'New York']
        for city in cities:
            for i in range(N_BATCHES):
                trips_batch = []
                for j in range(N_TRIPS_PER_BATCH):
                    trips_batch.append(Trip(
                        start_date='2016-04-03',
                        start_station_code=2,
                        end_date='2016-04-03',
                        end_station_code=3,
                        duration_sec=1,
                        is_member=False,
                        year_id=2016
                    ))
                trips_message = pickle.dumps((trips_batch, city))
                storage_handler.prepare(trips_message)
                storage_handler.commit()

        uncommited_trips_batch = [Trip(
            start_date='2017-04-03',
            start_station_code=3,
            end_date='2017-04-03',
            end_station_code=4,
            duration_sec=1,
            is_member=False,
            year_id=2017
        )]
        uncommited_trips_message = pickle.dumps((uncommited_trips_batch, 'Vermont'))
        storage_handler.prepare(uncommited_trips_message)

        expected_recovered_storage = {
            'Vermont': {'2': {'2016': 8, '2017': 0}},
            'Chicago': {'2': {'2016': 8, '2017': 0}},
            'New York': {'2': {'2016': 8, '2017': 0}}
        }

        recovered_storage_handler = StationCounterStorageHandler(storage_directory=LOGS_DIR, checkpoint_frequency=100)
        self.assertEqual(expected_recovered_storage, recovered_storage_handler.get_storage())


if __name__ == '__main__':
    unittest.main()
