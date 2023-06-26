import common.network.constants
import common.network.deserialize
import pickle


def weather_filter_process_input(message_type: bytes, message_body: bytes):
    if message_type == common.network.constants.WEATHER_BATCH:
        raw_batch, city = pickle.loads(message_body)
        weather_batch = common.network.deserialize.deserialize_weather_batch(raw_batch)

        filtered_weathers = []
        for weather in weather_batch:
            if weather.precipitations >= 30.0:
                filtered_weathers.append(weather)
        if len(filtered_weathers) == 0:
            return None
        else:
            return common.network.constants.WEATHER_BATCH + pickle.dumps((filtered_weathers, city))
