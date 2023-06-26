class Weather:
    def __init__(self, date, precipitations):
        self.date = date
        self.precipitations = precipitations


def to_dict(weather: Weather):
    return {
        'date': weather.date,
        'precipitations': weather.precipitations
    }


def from_dict(weather_dict: dict):
    return Weather(date=weather_dict['date'], precipitations=weather_dict['precipitations'])
