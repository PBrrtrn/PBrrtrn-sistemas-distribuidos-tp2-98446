FROM rabbitmq-python-base:0.0.1

COPY backend/montreal_stations_over_6km_avg_trip_distance_ingestor /
COPY common /common

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh"]