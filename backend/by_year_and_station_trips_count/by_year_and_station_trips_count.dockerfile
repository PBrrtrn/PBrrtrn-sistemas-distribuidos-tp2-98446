FROM rabbitmq-python-base:0.0.1

COPY backend/by_year_and_station_trips_count /
COPY common /common

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh"]