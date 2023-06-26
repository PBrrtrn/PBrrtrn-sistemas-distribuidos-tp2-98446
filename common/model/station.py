class Station:
    def __init__(self, code: int, name: str, latitude: float, longitude: float):
        self.code = code
        self.name = name
        self.latitude = latitude
        self.longitude = longitude


def to_dict(station: Station):
    return {
        'code': station.code,
        'name': station.name,
        'latitude': station.latitude,
        'longitude': station.longitude
    }


def from_dict(station_dict: dict):
    return Station(
        code=station_dict['code'],
        name=station_dict['name'],
        latitude=station_dict['latitude'],
        longitude=station_dict['longitude']
    )
