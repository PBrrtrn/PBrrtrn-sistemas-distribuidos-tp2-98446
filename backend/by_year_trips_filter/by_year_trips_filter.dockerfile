FROM rabbitmq-python-base:0.0.1

COPY backend/by_year_trips_filter /
COPY common /common
ENTRYPOINT ["/bin/sh"]