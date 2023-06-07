FROM rabbitmq-python-base:0.0.1

COPY backend/trip_duration_running_avg /
COPY common /common
ENTRYPOINT ["/bin/sh"]