FROM rabbitmq-python-base:0.0.1

COPY backend/doubled_yearly_trips_stations_ingestor /
COPY common /common

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh"]