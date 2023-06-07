FROM rabbitmq-python-base:0.0.1

COPY backend/stations_manager /
COPY common /common
ENTRYPOINT ["/bin/sh"]