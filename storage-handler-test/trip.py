class Trip:
    def __init__(self, start_date, start_station_code, end_date, end_station_code, duration_sec, is_member, year_id):
        self.start_date = start_date
        self.start_station_code = start_station_code
        self.end_date = end_date
        self.end_station_code = end_station_code
        self.duration_sec = duration_sec
        self.is_member = is_member
        self.year_id = year_id
