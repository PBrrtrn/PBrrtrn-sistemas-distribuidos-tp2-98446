FROM rabbitmq-python-base:0.0.1

COPY backend/montreal_stations_joiner /
COPY common /common
ENTRYPOINT ["/bin/sh"]